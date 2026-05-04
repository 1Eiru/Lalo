from flask import Flask, render_template, jsonify, request, abort, Response
import re 
from flask_limiter import Limiter
from flask_limiter.util import get_remote_address
from pymongo import MongoClient, ASCENDING, DESCENDING, TEXT 
from datetime import datetime, timezone
from flask_apscheduler import APScheduler
import requests, os, time, math

app = Flask(__name__)
scheduler = APScheduler()
scheduler.init_app(app)
scheduler.start()

# 1. Setup Rate Limiting
limiter = Limiter(
    get_remote_address,
    app=app,
    default_limits=["2000 per day", "200 per hour"], 
    storage_uri="memory://"
)

mongoURI = os.getenv('TEST_URI')
client = MongoClient(mongoURI)
db = client.flask_database

# Collections
events_collection = db.events
battles_collection = db.battles
battles_cache = db.battles_cache  
settings_collection = db.settings
cache_collection = db.player_cache

# Indexes
#events_collection.create_index([("CreatedAt", ASCENDING)], expireAfterSeconds=1800)
battles_collection.create_index([("endTime", DESCENDING)]) 
cache_collection.create_index([("last_updated", ASCENDING)], expireAfterSeconds=300)
battles_cache.create_index([("createdAt", ASCENDING)], expireAfterSeconds=3600)

battles_collection.create_index([
    ("player_names", TEXT),
    ("guild_names", TEXT)
], name="battle_search_index")

def get_scheduler_status(key='scheduler_status'):
    status = settings_collection.find_one({'_id': key})
    if not status:
        return {'last_check': 'Waiting...', 'next_run': time.time()}
    return status

# --- EVENTS LOGIC ---
def fetch_event_details(event_id):
    try:
        response = requests.get(f'https://gameinfo-sgp.albiononline.com/api/gameinfo/events/{event_id}', timeout=10)
        if response.status_code == 200:
            return response.json()
    except Exception: pass
    return None

# --- BATTLES LOGIC ---
def fetch_battles_data(sort_type='recent', time_range='week', limit=51, offset=0, max_fetch=1500):
    try:
        base_url = "https://gameinfo-sgp.albiononline.com/api/gameinfo/battles"       
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.159 Safari/537.36'
        }

        current_offset = offset
        total_fetched = 0

        print(f"Starting battle fetch: range={time_range}, sort={sort_type}, limit={limit}, max_fetch={max_fetch}")

        while total_fetched < max_fetch:
            params = [
                ('range', time_range),
                ('offset', current_offset),
                ('limit', limit),
                ('sort', sort_type)
            ]
            
            try:
                response = requests.get(base_url, params=params, headers=headers, timeout=15)
                
                if response.status_code != 200:
                    print(f"API Error {response.status_code} at offset {current_offset}. Stopping.")
                    break

                battles = response.json()
                
                if not battles:
                    print(f"No battles returned at offset {current_offset}. Stopping.")
                    break

                count = 0
                for b in battles:
                    if b.get('totalFame', 0) <= 100000:
                        continue
                    try:
                        t_str = b['endTime'].replace('Z', '')
                        if '.' in t_str:
                            main_part, frac_part = t_str.split('.')
                            t_str = f"{main_part}.{frac_part[:6]}"
                        end_time = datetime.fromisoformat(t_str).replace(tzinfo=timezone.utc)
                    except (ValueError, TypeError):
                        end_time = datetime.now(timezone.utc)
                    
                    try:
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
                
                print(f"Processed {count} valid battles at offset {current_offset} (Fetched {len(battles)} items).")
                
                total_fetched += len(battles)
                current_offset += limit
                
                if len(battles) < limit:
                    print("Reached end of battle history.")
                    break
                    
                time.sleep(1.5)

            except requests.exceptions.RequestException as e:
                print(f"Network error at offset {current_offset}: {e}")
                break

    except Exception as e:
        print(f"Critical error fetching battles: {e}")

def fetch_full_battle_history(battle_id):
    all_events = []
    offset = 0
    limit = 50
    
    print(f"Fetching full history for battle {battle_id}...")
    
    while True:
        success = False
        for attempt in range(3):
            try:
                url = f"https://gameinfo-sgp.albiononline.com/api/gameinfo/events/battle/{battle_id}"
                params = {'offset': offset, 'limit': limit}
                r = requests.get(url, params=params, timeout=10)
                
                if r.status_code == 200:
                    data = r.json()
                    success = True
                    break 
                elif r.status_code == 404:
                    success = True 
                    data = []
                    break
                else:
                    print(f"API Error {r.status_code} at offset {offset}. Retrying ({attempt+1}/3)...")
                    time.sleep(1.5)
            except Exception as e:
                print(f"Exception fetching battle events: {e}. Retrying ({attempt+1}/3)...")
                time.sleep(1.5)
        
        if not success:
            print(f"Failed to fetch offset {offset} after retries. Stopping.")
            break

        if not data:
            break
            
        all_events.extend(data)
        if len(data) < limit:
            break 
        offset += limit
        time.sleep(0.2)
            
    return all_events

# --- BATTLE DETAILS CACHE LOGIC ---

def minify_battle_event(event):
    def clean_item(item):
        if not item: return None
        return {
            'Type': item.get('Type'),
            'Count': item.get('Count', 1),
            'Quality': item.get('Quality', 1)
        }

    def clean_equipment(equip):
        if not equip: return None
        cleaned = {}
        for slot, item in equip.items():
            if item:
                cleaned[slot] = clean_item(item)
            else:
                cleaned[slot] = None
        return cleaned

    def clean_inventory(inv):
        if not inv: return []
        return [clean_item(i) for i in inv if i]

    k = event.get('Killer', {})
    v = event.get('Victim', {})

    return {
        'TotalVictimKillFame': event.get('TotalVictimKillFame', 0),
        'Killer': {
            'Name': k.get('Name', 'Unknown'),
            'GuildName': k.get('GuildName', ''),
            'AllianceName': k.get('AllianceName', ''),
            'Equipment': clean_equipment(k.get('Equipment'))
        },
        'Victim': {
            'Name': v.get('Name', 'Unknown'),
            'GuildName': v.get('GuildName', ''),
            'AllianceName': v.get('AllianceName', ''),
            'Equipment': clean_equipment(v.get('Equipment')),
            'Inventory': clean_inventory(v.get('Inventory'))
        }
    }

def get_battle_details_cached(battle_id):
    cached = battles_cache.find_one({'battle_id': battle_id})
    if cached:
        return cached['players']
    players_map = {}
    try:
        summary_url = f"https://gameinfo-sgp.albiononline.com/api/gameinfo/battles/{battle_id}"
        r_summary = requests.get(summary_url, timeout=10)
        if r_summary.status_code == 200:
            summary_data = r_summary.json()
            raw_players = summary_data.get('players', {})
            
            for pid, pdata in raw_players.items():
                players_map[str(pid)] = {
                    'Id': str(pdata.get('id')),
                    'Name': pdata.get('name', 'Unknown'),
                    'GuildName': pdata.get('guildName', ''),
                    'AllianceName': pdata.get('allianceName', ''),
                    'Kills': [], 
                    'DeathEvents': [],
                    'Deaths': pdata.get('deaths', 0),
                    'KillFame': pdata.get('killFame', 0),
                    'Damage': 0,  
                    'Healing': 0,
                    'IP': 0    
                }
    except Exception as e:
        print(f"Error fetching battle summary for player list: {e}")

    events = fetch_full_battle_history(battle_id)
    def init_player_if_missing(p_id, name, guild, alliance, ip=0):
        p_id = str(p_id)
        if p_id not in players_map:
            players_map[p_id] = {
                'Id': p_id,
                'Name': name,
                'GuildName': guild if guild else "",
                'AllianceName': alliance if alliance else "",
                'Kills': [],
                'DeathEvents': [],
                'Deaths': 0,
                'KillFame': 0,
                'Damage': 0,      
                'Healing': 0,    
                'IP': int(ip)     
            }
        else:
            if int(ip) > players_map[p_id]['IP']:
                players_map[p_id]['IP'] = int(ip)

    # 3. Process Events
    for e in events:
        k = e['Killer']
        v = e['Victim']
        parts = e.get('Participants', [])
        mini_event = minify_battle_event(e)

        # Process Killer
        init_player_if_missing(k['Id'], k['Name'], k.get('GuildName'), k.get('AllianceName'), k.get('AverageItemPower', 0))
        players_map[str(k['Id'])]['Kills'].append(mini_event)
        if players_map[str(k['Id'])]['KillFame'] == 0:
             players_map[str(k['Id'])]['KillFame'] += e.get('TotalVictimKillFame', 0)
        
        # Process Victim
        init_player_if_missing(v['Id'], v['Name'], v.get('GuildName'), v.get('AllianceName'), v.get('AverageItemPower', 0))
        players_map[str(v['Id'])]['DeathEvents'].append(mini_event)
        if players_map[str(v['Id'])]['Deaths'] == 0:
            players_map[str(v['Id'])]['Deaths'] += 1
        players_map[str(v['Id'])]['Deaths'] = max(players_map[str(v['Id'])]['Deaths'], len(players_map[str(v['Id'])]['DeathEvents']))

        for p in parts:
            init_player_if_missing(p['Id'], p['Name'], p.get('GuildName'), p.get('AllianceName'), p.get('AverageItemPower', 0))
            players_map[str(p['Id'])]['Damage'] += int(p.get('DamageDone', 0))
            players_map[str(p['Id'])]['Healing'] += int(p.get('SupportHealingDone', 0))

    players_list = list(players_map.values())
    if players_list:
        try:
            battles_cache.update_one(
                {'battle_id': battle_id},
                {'$set': {'battle_id': battle_id, 'createdAt': datetime.now(timezone.utc), 'players': players_list}},
                upsert=True
            )
        except Exception as e:
            print(f"Error caching battle {battle_id}: {e}")
    
    return players_list

# --- SCHEDULERS ---
@scheduler.task('interval', id='battle_check', seconds=90, misfire_grace_time=900)
def scheduled_update_battles():
    with app.app_context():
        print("Scheduler: Battles running...")
        current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        next_run = time.time() + 90 
        
        settings_collection.update_one(
            {'_id': 'battle_scheduler_status'},
            {'$set': {'last_check': current_time, 'next_run': next_run}},
            upsert=True
        )
        fetch_battles_data(sort_type='recent', time_range='week', limit=51)

# --- HELPERS ---
def fetch_player_from_api(player_id):
    player_info = {}
    kills = []
    deaths = []
    try:
        r_stats = requests.get(f'https://gameinfo-sgp.albiononline.com/api/gameinfo/players/{player_id}', timeout=5)
        if r_stats.status_code == 200: player_info = r_stats.json()
    except Exception: pass
    try:
        r_kills = requests.get(f'https://gameinfo-sgp.albiononline.com/api/gameinfo/players/{player_id}/kills', timeout=5)
        if r_kills.status_code == 200: kills = r_kills.json()
    except Exception: pass
    try:
        r_deaths = requests.get(f'https://gameinfo-sgp.albiononline.com/api/gameinfo/players/{player_id}/deaths', timeout=5)
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
        resp = requests.get(url, timeout=5)
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

@app.route("/battles")
def battles():
    page = request.args.get('page', 1, type=int)
    if page > 200: 
        page = 200
    search_query = request.args.get('search', '').strip()
    per_page = 25
    skip_amount = (page - 1) * per_page
    base_query = {'totalFame': {'$gt': 100000}}
    
    if search_query:
        search_filter = {}
        if search_query.isdigit():
             search_filter = {
                "$or": [
                    {'id': int(search_query)},
                    {"player_names": {"$regex": search_query, "$options": "i"}},
                    {"guild_names": {"$regex": search_query, "$options": "i"}}
                ]
            }
        else:
            search_filter = {
                "$or": [
                    {"player_names": {"$regex": search_query, "$options": "i"}},
                    {"guild_names": {"$regex": search_query, "$options": "i"}}
                ]
            }
        mongo_query = {"$and": [base_query, search_filter]}
    else:
        mongo_query = base_query
            
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

@app.route("/")
@app.route("/home")
def home():
    return battles()

@app.route("/events/<int:event_id>")
def events(event_id):
    data = list(events_collection.find({'EventId': event_id}))
    if not data:
        details = fetch_event_details(event_id)
        if details: data = [details]
    
    estimated_loss = 0
    if data: 
        if 'EstimatedVictimLootValue' in data[0]:
            estimated_loss = data[0]['EstimatedVictimLootValue']
        else:
            estimated_loss = calculate_estimated_loss(data[0]['Victim'])
            
    return render_template('events.html', events=data, estimated_loss=estimated_loss)

@app.route('/api/battles_list')
@limiter.limit("60 per minute")
def api_battles_list():
    try:
        page = int(request.args.get('page', 1))
    except ValueError:
        page = 1
    
    if page < 1: page = 1
    if page > 200: return jsonify({'error': 'Page limit exceeded. Please refine your search.'}), 400

    search_query = request.args.get('search', '').strip().replace('"', '')
    
    if search_query and not search_query.isdigit() and len(search_query) < 3:
        return jsonify({'error': 'Search query must be at least 3 characters.'}), 400

    per_page = 25
    base_query = {'totalFame': {'$gt': 100000}}

    if search_query:
        if search_query.isdigit():
             search_filter = {
                "$or": [
                    {'id': int(search_query)},
                    {"player_names": {"$regex": search_query, "$options": "i"}},
                    {"guild_names": {"$regex": search_query, "$options": "i"}}
                ]
            }
             mongo_query = {"$and": [base_query, search_filter]}
        else:
            search_filter = {
                "$or": [
                    {"player_names": {"$regex": search_query, "$options": "i"}},
                    {"guild_names": {"$regex": search_query, "$options": "i"}}
                ]
            }
            mongo_query = {"$and": [base_query, search_filter]}
    else:
        mongo_query = base_query

    total_battles = battles_collection.count_documents(mongo_query)
    total_pages = max(1, math.ceil(total_battles / per_page))
    if page > total_pages: page = total_pages
    skip_amount = (page - 1) * per_page
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
            r = requests.get(url, timeout=10)
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
    
    alliance_player_counts = {}
    if 'guilds' in battle:
        for g in battle['guilds'].values():
            aid = g.get('allianceId')
            p_count = g.get('playerCount', 0)
            if aid:
                alliance_player_counts[aid] = alliance_player_counts.get(aid, 0) + p_count

    alliances_list = []
    if 'alliances' in battle:
        for aid, adata in battle['alliances'].items():
            adata['playerCount'] = alliance_player_counts.get(aid, 0)
            alliances_list.append(adata)
    
    alliances_list.sort(key=lambda x: x.get('killFame', 0) or 0, reverse=True)

    guilds_list = []
    if 'guilds' in battle:
        guilds_list = list(battle['guilds'].values())

    guilds_list.sort(key=lambda x: x.get('killFame', 0) or 0, reverse=True)

    # --- Players Logic ---
    all_player_stats = get_battle_details_cached(battle_id)
    all_player_stats.sort(key=lambda x: x.get('KillFame', 0) or 0, reverse=True)
    total_players_count = len(all_player_stats)

    unique_guilds = sorted(list(set(p['GuildName'] for p in all_player_stats if p['GuildName'])))
    unique_alliances = sorted(list(set(p['AllianceName'] for p in all_player_stats if p['AllianceName'])))

    return render_template('battle_details.html', 
                           battle=battle, 
                           guilds=guilds_list, # Pass full list
                           alliances=alliances_list,                           
                           all_player_stats=all_player_stats, 
                           total_players_count=total_players_count,
                           
                           unique_guilds=unique_guilds,
                           unique_alliances=unique_alliances)

@app.route("/player/<player_id>")
def player(player_id):
    data = get_player_data(player_id)
    return render_template('player.html', **data)

@app.route('/api/search')
@limiter.limit("20 per minute")
def search_proxy():
    query = request.args.get('q', '')
    
    # Validation
    if not query or len(query) < 2: 
        return jsonify({'players': []})
    
    # Optional: Whitelist characters (Only allow letters, numbers, spaces)
    if not re.match("^[a-zA-Z0-9 ]+$", query):
        return jsonify({'players': []})

    try:
        url = f"https://gameinfo-sgp.albiononline.com/api/gameinfo/search?q={query}"
        headers = {'User-Agent': 'Mozilla/5.0'} 
        response = requests.get(url, headers=headers, timeout=3) 
        if response.status_code == 200: return jsonify(response.json())     
    except Exception: pass   
    return jsonify({'players': []})

@app.route('/robots.txt')
def robots_txt():
    content = """User-agent: *
Disallow: /api/
Disallow: /events/
Disallow: /battles/
Disallow: /player/

User-agent: ClaudeBot
Disallow: /

User-agent: GPTBot
Disallow: /

User-agent: ChatGPT-User
Disallow: /

User-agent: CCBot
Disallow: /

User-agent: anthropic-ai
Disallow: /

User-agent: Google-Extended
Disallow: /
"""
    return Response(content, mimetype='text/plain')

if __name__ == '__main__':
    app.run(threaded=True)