from flask import Flask, render_template, url_for, jsonify, request
from pymongo import MongoClient, ASCENDING, DESCENDING
from datetime import datetime, timedelta, timezone
from flask_apscheduler import APScheduler
import requests, json, os, time, math

app = Flask(__name__)
scheduler = APScheduler()
scheduler.init_app(app)
scheduler.start()

mongoURI = os.getenv('MONGODB_URI')
client = MongoClient(mongoURI)
db = client.flask_database

# Collections
events_collection = db.events
battles_collection = db.battles
battles_cache = db.battles_cache  
settings_collection = db.settings
cache_collection = db.player_cache

# Indexes
events_collection.create_index([("CreatedAt", ASCENDING)], expireAfterSeconds=1800)
battles_collection.create_index([("endTime", DESCENDING)]) 
cache_collection.create_index([("last_updated", ASCENDING)], expireAfterSeconds=300)
battles_cache.create_index([("createdAt", ASCENDING)], expireAfterSeconds=3600)

last_fetched_ids = set()

def load_existing_ids():
    global last_fetched_ids
    try:
        existing = events_collection.find({}, {'EventId': 1})
        last_fetched_ids = set(doc['EventId'] for doc in existing)
        print(f"Loaded {len(last_fetched_ids)} existing events from DB.")
    except Exception as e:
        print(f"Error loading existing IDs: {e}")

load_existing_ids()

def get_scheduler_status(key='scheduler_status'):
    status = settings_collection.find_one({'_id': key})
    if not status:
        return {'last_check': 'Waiting...', 'next_run': time.time()}
    return status

# --- EVENTS LOGIC ---
def fetch_event_ids():
    try:
        response = requests.get('https://gameinfo-sgp.albiononline.com/api/gameinfo/events', params={'offset': 0})
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
    except Exception: pass
    return None

def fetch_and_check_events():
    global last_fetched_ids
    current_ids = fetch_event_ids()
    if not current_ids: return
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

# --- BATTLES LOGIC ---
def fetch_battles_data():
    try:
        url = "https://gameinfo-sgp.albiononline.com/api/gameinfo/battles?sort=recent&offset=0&limit=50"
        response = requests.get(url)
        if response.status_code == 200:
            battles = response.json()
            count = 0
            for b in battles:
                try:
                    try:
                        end_time = datetime.fromisoformat(b['endTime'].replace('Z', '+00:00'))
                    except (ValueError, TypeError):
                        end_time = datetime.now()
                    guild_player_counts = {}
                    players_dict = b.get('players', {})
                    total_players = len(players_dict)
                    searchable_player_names = []
                    
                    for p_id, p_data in players_dict.items():
                        g_id = p_data.get('guildId')
                        if g_id:
                            guild_player_counts[g_id] = guild_player_counts.get(g_id, 0) + 1
                    
                        if p_data.get('name'):
                            searchable_player_names.append(p_data['name'])
                            
                    processed_guilds = {}
                    searchable_guild_names = []
                    
                    if 'guilds' in b:
                        for gid, gdata in b['guilds'].items():
                            g_name = gdata.get('name')
                            if g_name:
                                searchable_guild_names.append(g_name)
                                
                            processed_guilds[gid] = {
                                'name': g_name,
                                'kills': gdata.get('kills', 0),
                                'deaths': gdata.get('deaths', 0),
                                'killFame': gdata.get('killFame', 0),
                                'alliance': gdata.get('alliance'),
                                'allianceId': gdata.get('allianceId'),
                                'id': gdata.get('id'),
                                'playerCount': guild_player_counts.get(gid, 0)
                            }

                    processed_alliances = {}
                    if 'alliances' in b:
                        for aid, adata in b['alliances'].items():
                            processed_alliances[aid] = {
                                'name': adata.get('name'),
                                'kills': adata.get('kills', 0),
                                'deaths': adata.get('deaths', 0),
                                'killFame': adata.get('killFame', 0),
                                'id': adata.get('id')
                            }

                    battle_doc = {
                        'id': b['id'],
                        'totalFame': b.get('totalFame', 0),
                        'totalKills': b.get('totalKills', 0),
                        'endTime': end_time,
                        'totalPlayers': total_players,
                        'guilds': processed_guilds,
                        'alliances': processed_alliances,
                        'player_names': searchable_player_names, 
                        'guild_names': searchable_guild_names
                    }

                    battles_collection.update_one(
                        {'id': b['id']},
                        {'$set': battle_doc},
                        upsert=True
                    )
                    count += 1
                except Exception as e:
                    print(f"Error processing battle {b.get('id')}: {e}")
            print(f"Processed {count} battles.")
    except Exception as e:
        print(f"Error fetching battles: {e}")

def fetch_full_battle_history(battle_id):
    """Loops through offsets to get ALL events for a battle."""
    all_events = []
    offset = 0
    limit = 50
    
    print(f"Fetching full history for battle {battle_id}...")
    
    while True:
        try:
            url = f"https://gameinfo-sgp.albiononline.com/api/gameinfo/events/battle/{battle_id}"
            params = {'offset': offset, 'limit': limit}
            r = requests.get(url, params=params)
            
            if r.status_code != 200:
                print(f"API Error {r.status_code} at offset {offset}")
                break 
            data = r.json()
            if not data:
                break  
            all_events.extend(data)
            if len(data) < limit:
                break 
            offset += limit
            time.sleep(0.1)
        except Exception as e:
            print(f"Exception fetching battle events: {e}")
            break
            
    return all_events

# --- BATTLE DETAILS CACHE LOGIC ---

def get_battle_details_cached(battle_id):
    cached = battles_cache.find_one({'battle_id': battle_id})
    if cached:
        return cached['players']
    events = fetch_full_battle_history(battle_id)
    players_map = {}
    def init_player(p_id, name, guild, alliance, ip=0):
        if p_id not in players_map:
            players_map[p_id] = {
                'Id': p_id,
                'Name': name,
                'GuildName': guild,
                'AllianceName': alliance,
                'Kills': [],     
                'Deaths': 0,
                'KillFame': 0,
                'Damage': 0,      
                'Healing': 0,    
                'IP': int(ip)     
            }
        else:
            if int(ip) > players_map[p_id]['IP']:
                players_map[p_id]['IP'] = int(ip)
    for e in events:
        k = e['Killer']
        v = e['Victim']
        parts = e.get('Participants', [])
        init_player(k['Id'], k['Name'], k['GuildName'], k['AllianceName'], k.get('AverageItemPower', 0))
        players_map[k['Id']]['Kills'].append(e)
        players_map[k['Id']]['KillFame'] += e.get('TotalVictimKillFame', 0)
        init_player(v['Id'], v['Name'], v['GuildName'], v['AllianceName'], v.get('AverageItemPower', 0))
        players_map[v['Id']]['Deaths'] += 1

        for p in parts:
            init_player(p['Id'], p['Name'], p['GuildName'], p['AllianceName'], p.get('AverageItemPower', 0))
            players_map[p['Id']]['Damage'] += int(p.get('DamageDone', 0))
            players_map[p['Id']]['Healing'] += int(p.get('SupportHealingDone', 0))

    players_list = list(players_map.values())
    battles_cache.insert_one({
        'battle_id': battle_id,
        'createdAt': datetime.now(timezone.utc),
        'players': players_list
    })
    
    return players_list

# --- SCHEDULERS ---
@scheduler.task('interval', id='regular_check', seconds=30, misfire_grace_time=900)
def scheduled_update_event():
    with app.app_context():
        fetch_and_check_events()
        current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        next_run = time.time() + 30 
        settings_collection.update_one(
            {'_id': 'scheduler_status'},
            {'$set': {'last_check': current_time, 'next_run': next_run}},
            upsert=True
        )

@scheduler.task('interval', id='battle_check', seconds=90, misfire_grace_time=900)
def scheduled_update_battles():
    with app.app_context():
        print("Scheduler: Battles running...")
        fetch_battles_data()
        current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        next_run = time.time() + 90
        settings_collection.update_one(
            {'_id': 'battle_scheduler_status'},
            {'$set': {'last_check': current_time, 'next_run': next_run}},
            upsert=True
        )

# --- HELPERS ---
def get_latest_events():
    datas = events_collection.find().sort("TimeStamp", -1).limit(50)
    processed_datas = []
    for event in datas:
        try:
            timestamp = datetime.fromisoformat(event['TimeStamp'].replace('Z', '+00:00'))
        except (ValueError, TypeError):
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
    player_info = {}
    kills = []
    deaths = []
    try:
        r_stats = requests.get(f'https://gameinfo-sgp.albiononline.com/api/gameinfo/players/{player_id}')
        if r_stats.status_code == 200: player_info = r_stats.json()
    except Exception: pass
    try:
        r_kills = requests.get(f'https://gameinfo-sgp.albiononline.com/api/gameinfo/players/{player_id}/kills')
        if r_kills.status_code == 200: kills = r_kills.json()
    except Exception: pass
    try:
        r_deaths = requests.get(f'https://gameinfo-sgp.albiononline.com/api/gameinfo/players/{player_id}/deaths')
        if r_deaths.status_code == 200: deaths = r_deaths.json()
    except Exception: pass

    if not player_info:
        if kills:
            p = kills[0]['Killer']
            player_info = {'Name': p['Name'], 'GuildName': p['GuildName'], 'KillFame': p['KillFame'], 'DeathFame': p['DeathFame'], 'LifetimeStatistics': None}
        elif deaths:
            p = deaths[0]['Victim']
            player_info = {'Name': p['Name'], 'GuildName': p['GuildName'], 'KillFame': p['KillFame'], 'DeathFame': p['DeathFame'], 'LifetimeStatistics': None}
        else:
            player_info = {'Name': 'Unknown', 'GuildName': '-', 'KillFame': 0, 'DeathFame': 0, 'LifetimeStatistics': None}

    return {'player': player_info, 'kills': kills, 'deaths': deaths}

def get_player_data(player_id):
    cached = cache_collection.find_one({'player_id': player_id})
    if cached: return cached['data']
    template_data = fetch_player_from_api(player_id)
    cache_collection.update_one(
        {'player_id': player_id},
        {'$set': {'player_id': player_id, 'data': template_data, 'last_updated': datetime.now(timezone.utc)}},
        upsert=True
    )
    return template_data

def calculate_estimated_loss(victim_data):
    items_to_fetch = set()
    all_items = []
    if victim_data.get('Equipment'):
        for key, item in victim_data['Equipment'].items():
            if item:
                items_to_fetch.add(item['Type'])
                all_items.append((item['Type'], item['Quality'], item['Count']))
    if victim_data.get('Inventory'):
        for item in victim_data['Inventory']:
            if item:
                items_to_fetch.add(item['Type'])
                all_items.append((item['Type'], item['Quality'], item['Count']))
    if not items_to_fetch: return 0
    locations = "Caerleon,Bridgewatch,Martlock,Thetford,Lymhurst,Fortsterling"
    item_str = ",".join(items_to_fetch)
    url = f"https://east.albion-online-data.com/api/v2/stats/prices/{item_str}.json?locations={locations}&qualities=1,2,3,4,5"
    try:
        resp = requests.get(url)
        if resp.status_code != 200: return 0
        price_data = resp.json()
    except Exception: return 0
    
    price_map = {} 
    for entry in price_data:
        key = (entry['item_id'], entry['quality'])
        if key not in price_map: price_map[key] = {'sells': [], 'buys': []}
        if entry.get('sell_price_min', 0) > 0: price_map[key]['sells'].append(entry['sell_price_min'])
        if entry.get('buy_price_max', 0) > 0: price_map[key]['buys'].append(entry['buy_price_max'])
            
    total_est_value = 0
    for i_id, i_qual, i_count in all_items:
        key = (i_id, i_qual)
        unit_price = 0
        if key in price_map:
            data = price_map[key]
            if data['sells']:
                data['sells'].sort()
                cutoff = max(1, len(data['sells']) // 2) 
                low_end_prices = data['sells'][:cutoff]
                unit_price = sum(low_end_prices) / len(low_end_prices)
            elif data['buys']:
                unit_price = max(data['buys'])
        total_est_value += (unit_price * i_count)
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
    return render_template('home.html', datas=processed_datas, last_update=status.get('last_check'), next_update_ts=status.get('next_run'))

@app.route("/events/<int:event_id>")
def events(event_id):
    data = list(events_collection.find({'EventId': event_id}))
    if not data:
        details = fetch_event_details(event_id)
        if details: data = [details]
    estimated_loss = 0
    if data: estimated_loss = calculate_estimated_loss(data[0]['Victim'])
    return render_template('events.html', events=data, estimated_loss=estimated_loss)

# --- BATTLES LOGIC ---
def fetch_battles_data():
    try:
        url = "https://gameinfo-sgp.albiononline.com/api/gameinfo/battles?sort=recent&offset=0&limit=50"
        response = requests.get(url)
        if response.status_code == 200:
            battles = response.json()
            count = 0
            for b in battles:
                try:
                    try:
                        end_time = datetime.fromisoformat(b['endTime'].replace('Z', '+00:00'))
                    except (ValueError, TypeError):
                        end_time = datetime.now()

                    guild_player_counts = {}
                    players_dict = b.get('players', {})
                    total_players = len(players_dict)
                    
                    searchable_player_names = []
                    
                    for p_id, p_data in players_dict.items():
                        g_id = p_data.get('guildId')
                        if g_id:
                            guild_player_counts[g_id] = guild_player_counts.get(g_id, 0) + 1
                        
                        if p_data.get('name'):
                            searchable_player_names.append(p_data['name'])

                    processed_guilds = {}
                    searchable_guild_names = []
                    
                    if 'guilds' in b:
                        for gid, gdata in b['guilds'].items():
                            g_name = gdata.get('name')
                            if g_name:
                                searchable_guild_names.append(g_name)                     
                            processed_guilds[gid] = {
                                'name': g_name,
                                'kills': gdata.get('kills', 0),
                                'deaths': gdata.get('deaths', 0),
                                'killFame': gdata.get('killFame', 0),
                                'alliance': gdata.get('alliance'),
                                'allianceId': gdata.get('allianceId'),
                                'id': gdata.get('id'),
                                'playerCount': guild_player_counts.get(gid, 0)
                            }

                    processed_alliances = {}
                    if 'alliances' in b:
                        for aid, adata in b['alliances'].items():
                            processed_alliances[aid] = {
                                'name': adata.get('name'),
                                'kills': adata.get('kills', 0),
                                'deaths': adata.get('deaths', 0),
                                'killFame': adata.get('killFame', 0),
                                'id': adata.get('id')
                            }

                    battle_doc = {
                        'id': b['id'],
                        'totalFame': b.get('totalFame', 0),
                        'totalKills': b.get('totalKills', 0),
                        'endTime': end_time,
                        'totalPlayers': total_players,
                        'guilds': processed_guilds,
                        'alliances': processed_alliances,
                        'player_names': searchable_player_names, 
                        'guild_names': searchable_guild_names
                    }

                    battles_collection.update_one(
                        {'id': b['id']},
                        {'$set': battle_doc},
                        upsert=True
                    )
                    count += 1
                except Exception as e:
                    print(f"Error processing battle {b.get('id')}: {e}")
            print(f"Processed {count} battles.")
    except Exception as e:
        print(f"Error fetching battles: {e}")

@app.route("/battles")
def battles():
    page = request.args.get('page', 1, type=int)
    search_query = request.args.get('search', '').strip()
    per_page = 25
    skip_amount = (page - 1) * per_page
    mongo_query = {}
    if search_query:
        if search_query.isdigit():
             mongo_query = {'id': int(search_query)}
        else:
            regex = {"$regex": search_query, "$options": "i"} 
            mongo_query = {
                "$or": [
                    {"player_names": regex},
                    {"guild_names": regex}
                ]
            }
    total_battles = battles_collection.count_documents(mongo_query)
    total_pages = math.ceil(total_battles / per_page)
    battles_cursor = battles_collection.find(mongo_query).sort("endTime", -1).skip(skip_amount).limit(per_page)
    battles_data = list(battles_cursor)
    status = get_scheduler_status('battle_scheduler_status')
    return render_template('battles.html', 
                           battles=battles_data, 
                           page=page, 
                           total_pages=total_pages, 
                           search_query=search_query,
                           last_update=status.get('last_check'), 
                           next_update_ts=status.get('next_run'))

@app.route('/api/battles_list')
def api_battles_list():
    page = request.args.get('page', 1, type=int)
    search_query = request.args.get('search', '').strip()
    per_page = 25
    skip_amount = (page - 1) * per_page
    mongo_query = {}
    if search_query:
        if search_query.isdigit():
             mongo_query = {'id': int(search_query)}
        else:
            regex = {"$regex": search_query, "$options": "i"}
            mongo_query = {
                "$or": [
                    {"player_names": regex},
                    {"guild_names": regex}
                ]
            }

    battles_cursor = battles_collection.find(mongo_query).sort("endTime", -1).skip(skip_amount).limit(per_page)
    battles_data = []
    
    for b in battles_cursor:
        guild_names = b.get('guild_names', [])
        if not guild_names and 'guilds' in b:
             guild_names = [g['name'] for g in b['guilds'].values() if g.get('name')]

        ts = b['endTime']
        ts_str = ts.strftime('%Y-%m-%d %H:%M:%S') if isinstance(ts, datetime) else str(ts).replace('T', ' ').split('.')[0]
        
        battles_data.append({
            'id': b['id'],
            'endTime': ts_str,
            'totalPlayers': b.get('totalPlayers', 0),
            'totalFame': b.get('totalFame', 0),
            'guild_names': guild_names
        })
        
    status = get_scheduler_status('battle_scheduler_status')
    total_battles = battles_collection.count_documents(mongo_query)
    total_pages = math.ceil(total_battles / per_page)

    return jsonify({
        'battles': battles_data, 
        'last_update': status.get('last_check'), 
        'next_update_ts': status.get('next_run'),
        'total_pages': total_pages,
        'current_page': page
    })

@app.route("/battles/<int:battle_id>")
def battle_details(battle_id):
    battle = battles_collection.find_one({'id': battle_id})
    if not battle:
        try:
            url = f"https://gameinfo-sgp.albiononline.com/api/gameinfo/battles/{battle_id}"
            r = requests.get(url)
            if r.status_code == 200: 
                battle = r.json()
                if 'players' in battle and 'guilds' in battle:
                    counts = {}
                    for p in battle['players'].values():
                        gid = p.get('guildId')
                        counts[gid] = counts.get(gid, 0) + 1
                    for gid, gdata in battle['guilds'].items():
                        gdata['playerCount'] = counts.get(gid, 0)
        except Exception: pass

    if not battle:
        return "Battle not found", 404
    guilds_list = []
    if 'guilds' in battle:
        guilds_list = list(battle['guilds'].values())

    g_sort = request.args.get('sort', 'fame')
    g_order = request.args.get('order', 'desc')
    g_key_map = {'fame': 'killFame', 'kills': 'kills', 'deaths': 'deaths', 'players': 'playerCount'}
    g_sort_key = g_key_map.get(g_sort, 'killFame')
    
    guilds_list.sort(key=lambda x: x.get(g_sort_key, 0) or 0, reverse=(g_order == 'desc'))

    page = request.args.get('page', 1, type=int)
    per_page = 10
    total_guilds = len(guilds_list)
    total_pages = math.ceil(total_guilds / per_page)
    start = (page - 1) * per_page
    end = start + per_page
    paginated_guilds = guilds_list[start:end]


    player_stats = get_battle_details_cached(battle_id)
    
    # Player Sort
    p_sort = request.args.get('p_sort', 'fame')
    p_order = request.args.get('p_order', 'desc')
    

    p_key_map = {
        'fame': 'KillFame',
        'kills': 'Kills', 
        'deaths': 'Deaths',
        'damage': 'Damage',
        'healing': 'Healing',
        'ip': 'IP'
    }
    p_sort_key = p_key_map.get(p_sort, 'KillFame')
    p_reverse = (p_order == 'desc')

    def player_sorter(x):
        val = x.get(p_sort_key, 0)
        if p_sort == 'kills' and isinstance(val, list):
            return len(val)
        return val or 0

    player_stats.sort(key=player_sorter, reverse=p_reverse)
    unique_guilds = sorted(list(set(p['GuildName'] for p in player_stats if p['GuildName'])))
    unique_alliances = sorted(list(set(p['AllianceName'] for p in player_stats if p['AllianceName'])))

    return render_template('battle_details.html', 
                           battle=battle, 
                           guilds=paginated_guilds, 
                           page=page, 
                           total_pages=total_pages,
                           current_sort=g_sort,
                           current_order=g_order,
                           player_stats=player_stats,
                           p_sort=p_sort,
                           p_order=p_order,
                           unique_guilds=unique_guilds,
                           unique_alliances=unique_alliances)

@app.route("/player/<player_id>")
def player(player_id):
    data = get_player_data(player_id)
    return render_template('player.html', **data)

@app.route('/api/updates')
def api_updates():
    latest_events = get_latest_events()
    status = get_scheduler_status()
    return jsonify({'events': latest_events, 'last_update': status.get('last_check'), 'next_update_ts': status.get('next_run')})

@app.route('/api/search')
def search_proxy():
    query = request.args.get('q', '')
    if not query or len(query) < 2: return jsonify({'players': []})
    try:
        url = f"https://gameinfo-sgp.albiononline.com/api/gameinfo/search?q={query}"
        headers = {'User-Agent': 'Mozilla/5.0'} 
        response = requests.get(url, headers=headers)
        if response.status_code == 200: return jsonify(response.json())     
    except Exception: pass   
    return jsonify({'players': []})

if __name__ == '__main__':
    app.run(threaded=True)