from flask import Flask, render_template, url_for, jsonify, request
from pymongo import MongoClient, ASCENDING
from datetime import datetime, timedelta, timezone
from flask_apscheduler import APScheduler
import requests, json, os, time

app = Flask(__name__)
scheduler = APScheduler()
scheduler.init_app(app)
scheduler.start()

mongoURI = os.getenv('MONGODB_URI')
client = MongoClient(mongoURI)
db = client.flask_database

# Collections
events_collection = db.events
settings_collection = db.settings
cache_collection = db.player_cache

# Indexes
events_collection.create_index([("CreatedAt", ASCENDING)], expireAfterSeconds=36000)

# CACHE
CACHE_DURATION = 120 
cache_collection.create_index([("last_updated", ASCENDING)], expireAfterSeconds=CACHE_DURATION)

last_fetched_ids = set()

def load_existing_ids():
    """Load existing EventIds from DB on startup to avoid API spam."""
    global last_fetched_ids
    try:
        existing = events_collection.find({}, {'EventId': 1})
        last_fetched_ids = set(doc['EventId'] for doc in existing)
        print(f"Loaded {len(last_fetched_ids)} existing events from DB.")
    except Exception as e:
        print(f"Error loading existing IDs: {e}")

load_existing_ids()

def get_scheduler_status():
    """Read the timer status from MongoDB."""
    status = settings_collection.find_one({'_id': 'scheduler_status'})
    if not status:
        return {
            'last_check': 'Waiting for first run...',
            'next_run': time.time() 
        }
    return status

def update_scheduler_status(last_check_time):
    """Save the timer status to MongoDB."""
    next_run = time.time() + 90
    settings_collection.update_one(
        {'_id': 'scheduler_status'},
        {'$set': {
            'last_check': last_check_time,
            'next_run': next_run
        }},
        upsert=True
    )

def fetch_event_ids():
    try:
        response = requests.get('https://gameinfo-sgp.albiononline.com/api/gameinfo/events')
        if response.status_code == 200:
            data = response.json()
            return set(event['EventId'] for event in data)
        return set()
    except Exception as e:
        print(f"Error fetching IDs: {e}")
        return set()

def fetch_event_details(event_id):
    try:
        response = requests.get(f'https://gameinfo-sgp.albiononline.com/api/gameinfo/events/{event_id}')
        if response.status_code == 200:
            return response.json()
    except Exception:
        pass
    return None

def fetch_and_check_events():
    global last_fetched_ids

    current_ids = fetch_event_ids()
    if not current_ids:
        return
    new_ids = current_ids - last_fetched_ids

    if new_ids:
        print(f"Found {len(new_ids)} new events. Fetching details...")
        for event_id in new_ids:
            event_details = fetch_event_details(event_id)
            if event_details:
                try:
                    try:
                        dt_object = datetime.fromisoformat(event_details['TimeStamp'].replace('Z', '+00:00'))
                    except ValueError:
                        dt_object = datetime.now()
                    event_details['CreatedAt'] = dt_object

                    events_collection.update_one(
                        {'EventId': event_details['EventId']},
                        {'$set': event_details},
                        upsert=True,
                    )
                    last_fetched_ids.add(event_id)
                except Exception as e:
                    print(f"DB Error: {e}")
        print("Update complete.")
    else:
        print("No new events found.")

@scheduler.task('interval', id='regular_check', seconds=30, misfire_grace_time=900)
def scheduled_update_event():
    with app.app_context():
        print("Scheduler running...")
        fetch_and_check_events()
        
        current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        next_run = time.time() + 30 
        settings_collection.update_one(
            {'_id': 'scheduler_status'},
            {'$set': {
                'last_check': current_time,
                'next_run': next_run
            }},
            upsert=True
        )

def get_latest_events():
    datas = events_collection.find().sort("TimeStamp", -1).limit(50)
    processed_datas = []
    for event in datas:
        try:
            timestamp = datetime.fromisoformat(event['TimeStamp'].replace('Z', '+00:00'))
        except ValueError:
            timestamp = event['TimeStamp']

        processed_event = {
            'EventId': event['EventId'],
            'TimeStamp': timestamp.strftime('%Y-%m-%d %H:%M:%S') if isinstance(timestamp, datetime) else str(timestamp),
            'KillerName': event['Killer']['Name'],
            'KillerId': event['Killer']['Id'],
            'VictimName': event['Victim']['Name'],
            'VictimId': event['Victim']['Id']
        }
        processed_datas.append(processed_event)
    return processed_datas

def fetch_player_from_api(player_id):
    """Handles the raw API calls to Albion Online."""
    player_info = {}
    kills = []
    deaths = []

    try:
        r_stats = requests.get(f'https://gameinfo-sgp.albiononline.com/api/gameinfo/players/{player_id}')
        if r_stats.status_code == 200:
            player_info = r_stats.json()
    except Exception as e:
        print(f"Error fetching player stats: {e}")

    try:
        r_kills = requests.get(f'https://gameinfo-sgp.albiononline.com/api/gameinfo/players/{player_id}/kills')
        if r_kills.status_code == 200:
            kills = r_kills.json()
    except Exception as e:
        print(f"Error fetching kills: {e}")

    try:
        r_deaths = requests.get(f'https://gameinfo-sgp.albiononline.com/api/gameinfo/players/{player_id}/deaths')
        if r_deaths.status_code == 200:
            deaths = r_deaths.json()
    except Exception as e:
        print(f"Error fetching deaths: {e}")

    if not player_info:
        if kills:
            p = kills[0]['Killer']
            player_info = {'Name': p['Name'], 'GuildName': p['GuildName'], 'KillFame': p['KillFame'], 'DeathFame': p['DeathFame'], 'LifetimeStatistics': None}
        elif deaths:
            p = deaths[0]['Victim']
            player_info = {'Name': p['Name'], 'GuildName': p['GuildName'], 'KillFame': p['KillFame'], 'DeathFame': p['DeathFame'], 'LifetimeStatistics': None}
        else:
            player_info = {'Name': 'Unknown', 'GuildName': '-', 'KillFame': 0, 'DeathFame': 0, 'LifetimeStatistics': None}

    return {
        'player': player_info,
        'kills': kills,
        'deaths': deaths
    }

def get_player_data(player_id):
    cached = cache_collection.find_one({'player_id': player_id})
    if cached:
        print(f"Serving {player_id} from MongoDB cache.")
        return cached['data']
    print(f"Fetching {player_id} from API...")
    template_data = fetch_player_from_api(player_id)
    
    cache_collection.update_one(
        {'player_id': player_id},
        {'$set': {
            'player_id': player_id,
            'data': template_data,
            'last_updated': datetime.now(timezone.utc)
        }},
        upsert=True
    )
    return template_data

def calculate_estimated_loss(victim_data):
    """
    Calculates estimated silver lost based on Equipment and Inventory.
    Uses East Albion Data API.
    """
    items_to_fetch = set()
    #(Type, Quality, Count)
    all_items = []

    #quipment
    if victim_data.get('Equipment'):
        for key, item in victim_data['Equipment'].items():
            if item:
                items_to_fetch.add(item['Type'])
                all_items.append((item['Type'], item['Quality'], item['Count']))
    # Inventory
    if victim_data.get('Inventory'):
        for item in victim_data['Inventory']:
            if item:
                items_to_fetch.add(item['Type'])
                all_items.append((item['Type'], item['Quality'], item['Count']))

    if not items_to_fetch:
        return 0

    locations = "Caerleon,Bridgewatch,Martlock,Thetford,Lymhurst,Fortsterling"
    item_str = ",".join(items_to_fetch)
    url = f"https://east.albion-online-data.com/api/v2/stats/prices/{item_str}.json?locations={locations}&qualities=1,2,3,4,5"

    try:
        resp = requests.get(url)
        if resp.status_code != 200:
            print(f"Price API Error: {resp.status_code}")
            return 0
        price_data = resp.json()
    except Exception as e:
        print(f"Price Fetch Error: {e}")
        return 0

    price_map = {} 
    for entry in price_data:
        p_min = entry.get('sell_price_min', 0)
        if p_min > 0:
            key = (entry['item_id'], entry['quality'])
            if key not in price_map:
                price_map[key] = []
            price_map[key].append(p_min)
    # Total
    total_est_value = 0
    for i_id, i_qual, i_count in all_items:
        key = (i_id, i_qual)
        if key in price_map:
            avg_price = sum(price_map[key]) / len(price_map[key])
            total_est_value += (avg_price * i_count)
        else:
            pass

    return int(total_est_value)

# --ROUTES ---
@app.route("/")
@app.route("/home")
def home():
    datas = events_collection.find().sort("TimeStamp", -1).limit(50)
    processed_datas = []
    for event in datas:
        try:
            timestamp = datetime.fromisoformat(event['TimeStamp'].replace('Z', '+00:00'))
        except ValueError:
            timestamp = event['TimeStamp']

        processed_event = {**event, 'TimeStamp': timestamp}
        processed_datas.append(processed_event)
    
    status = get_scheduler_status()
    
    return render_template('home.html', 
                           datas=processed_datas, 
                           last_update=status.get('last_check'),
                           next_update_ts=status.get('next_run'))

@app.route("/events/<int:event_id>")
def events(event_id):
    data = list(events_collection.find({'EventId': event_id}))
    if not data:
        details = fetch_event_details(event_id)
        if details:
            data = [details]
    estimated_loss = 0
    if data:
        estimated_loss = calculate_estimated_loss(data[0]['Victim'])
    return render_template('events.html', events=data, estimated_loss=estimated_loss)

@app.route("/player/<player_id>")
def player(player_id):
    data = get_player_data(player_id)
    return render_template('player.html', **data)

@app.route('/api/updates')
def api_updates():
    latest_events = get_latest_events()
    status = get_scheduler_status()
    return jsonify({
        'events': latest_events,
        'last_update': status.get('last_check'),
        'next_update_ts': status.get('next_run')
    })

@app.route('/api/search')
def search_proxy():
    query = request.args.get('q', '')
    if not query or len(query) < 2:
        return jsonify({'players': []})
    try:
        url = f"https://gameinfo-sgp.albiononline.com/api/gameinfo/search?q={query}"
        headers = {'User-Agent': 'Mozilla/5.0'} 
        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            return jsonify(response.json())
        else:
            print(f"API Error: {response.status_code}")       
    except Exception as e:
        print(f"Search Exception: {e}")   
    return jsonify({'players': []})

if __name__ == '__main__':
    app.run(threaded=True)