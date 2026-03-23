import discord
from discord.ext import commands, tasks
from pymongo import MongoClient, ASCENDING
from PIL import Image, ImageDraw, ImageFont
from io import BytesIO
import os
import aiohttp
import asyncio
from datetime import datetime, timezone, timedelta
from dotenv import load_dotenv 
import math

load_dotenv() 

# --- CONFIGURATION ---
TOKEN = os.getenv('DISCORD_BOT_TOKEN')
MONGO_URI = os.getenv('MONGODB_URI')
CHANNEL_ID = None
API_HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Accept': 'application/json, text/plain, */*',
    'Accept-Language': 'en-US,en;q=0.9'
}

# --- DATABASE ---
client = MongoClient(MONGO_URI)
db = client.flask_database
events_collection = db.events
settings_collection = db.settings
tracked_collection = db.tracked_players 

# Ensure indexes
events_collection.create_index([("EventId", ASCENDING)], unique=True)
events_collection.create_index([("processed", ASCENDING), ("EventId", ASCENDING)])
tracked_collection.create_index([("name", ASCENDING)], unique=True)

# --- ASSETS ---
try:
    FONT = ImageFont.truetype("arial.ttf", 24)
    SMALL_FONT = ImageFont.truetype("arial.ttf", 18)
    BOLD_FONT = ImageFont.truetype("arialbd.ttf", 28)
    LARGE_BOLD_FONT = ImageFont.truetype("arialbd.ttf", 40)
    XSMALL_FONT = ImageFont.truetype("arial.ttf", 16)
except:
    FONT = ImageFont.load_default()
    SMALL_FONT = ImageFont.load_default()
    BOLD_FONT = ImageFont.load_default()
    LARGE_BOLD_FONT = ImageFont.load_default()
    XSMALL_FONT = ImageFont.load_default()

def log(message):
    now = datetime.now().strftime('%H:%M:%S')
    print(f"[{now}] {message}")

# --- BOT CLASS ---
class KillboardBot(commands.Bot):
    def __init__(self):
        intents = discord.Intents.default()
        intents.message_content = True
        super().__init__(command_prefix="!", intents=intents)
        self.is_scanning = False

    async def setup_hook(self):
        self.ingest_events.start()
        self.process_events_queue.start()
        log("Ingestion and Processing loops started.")

    async def on_ready(self):
        log(f'Logged in as {self.user}')

# ---INGESTION [20s] ---
    @tasks.loop(seconds=20)
    async def ingest_events(self):
        await self.wait_until_ready()
        if self.is_scanning:
            log("Previous scan still running. Skipping this tick.")
            return
        self.is_scanning = True
        
        try:
            log("Starting Deep Scan Cycle...")  
            tracked_cursor = tracked_collection.find({}, {'name': 1})
            tracked_names = {doc['name'].lower() for doc in tracked_cursor}            
            log(f"Tracking {len(tracked_names)} players.")
            
            if not tracked_names:
                log("No players to track. Skipping API calls.")
                return 

            async with aiohttp.ClientSession() as session:
                for offset in range(0, 1001, 51):
                    try:
                        url = f"https://gameinfo-sgp.albiononline.com/api/gameinfo/events?limit=51&offset={offset}&sort=recent"
                        log(f"Scanning Offset {offset}...") 

                        async with session.get(url, headers=API_HEADERS) as resp:
                            if resp.status != 200:
                                log(f"API Error {resp.status} at offset {offset}")
                                break
                            
                            api_data = await resp.json()
                            if not api_data:
                                log("   -> End of stream.")
                                break

                            events_to_save = []
                            
                            for event in api_data:

                                if event.get('TotalVictimKillFame', 0) == 0:
                                    continue

                                eid = event['EventId']
                                k_name = event['Killer']['Name'].lower()
                                v_name = event['Victim']['Name'].lower()
                                
                                # Check participants for tracked players
                                participants = [p['Name'].lower() for p in event.get('Participants', [])]
                                is_tracked_participant = any(p in tracked_names for p in participants)
                                
                                if k_name in tracked_names or v_name in tracked_names or is_tracked_participant:
                                    if not events_collection.find_one({'EventId': eid}, {'_id': 1}):
                                        log(f"   -> Found NEW Tracked Event! ID: {eid} ({k_name} vs {v_name})")
                                        
                                        try:
                                            event['CreatedAt'] = datetime.fromisoformat(event['TimeStamp'].replace('Z', '+00:00'))
                                        except:
                                            event['CreatedAt'] = datetime.now(timezone.utc)
                                        
                                        event['processed'] = False
                                        events_to_save.append(event)

                            if events_to_save:
                                events_to_save.sort(key=lambda x: x['EventId'])
                                try:
                                    events_collection.insert_many(events_to_save, ordered=False)
                                    log(f"Saved {len(events_to_save)} events to DB.")
                                except Exception as e:
                                    log(f"   -> Insert warning: {e}")

                    except Exception as e:
                        log(f"Ingestion Exception at offset {offset}: {e}")
                        break
                    await asyncio.sleep(0.3)

            current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            next_run = datetime.now().timestamp() + 20
            settings_collection.update_one(
                {'_id': 'scheduler_status'},
                {'$set': {'last_check': current_time, 'next_run': next_run}},
                upsert=True
            )
            log("Scan Complete.")
            
        finally:
            self.is_scanning = False


# ---PROCESSING ---
    @tasks.loop(seconds=5) 
    async def process_events_queue(self):
        await self.wait_until_ready()

        global CHANNEL_ID
        if CHANNEL_ID is None:
            config = settings_collection.find_one({'_id': 'config'})
            if config and 'channel_id' in config:
                CHANNEL_ID = config['channel_id']
            else:
                return 

        queue = list(events_collection.find({'processed': False}).sort('EventId', 1).limit(5))
        if not queue: return

        log(f"Processing Queue: {len(queue)} pending events...")
        
        channel = self.get_channel(CHANNEL_ID)
        if not channel: 
            log(f"Error: Discord Channel ID {CHANNEL_ID} not found or bot lacks access.")
            return

        async with aiohttp.ClientSession() as session:
            for event in queue:
                await self.process_single_event(session, channel, event)

    async def process_single_event(self, session, channel, event):
        eid = event['EventId']
        try:
            log(f"   -> Processing Event #{eid}")
            
            if 'EstimatedVictimLootValue' not in event:
                est_value = await get_estimated_value(session, event['Victim'])
                event['EstimatedVictimLootValue'] = est_value
            else:
                est_value = event['EstimatedVictimLootValue']

            img_bytes = await generate_versus_image(session, event)
            
            file = discord.File(img_bytes, filename="killboard.png")
            embed = create_embed(event, est_value)
            await channel.send(embed=embed, file=file)
            
            events_collection.update_one(
                {'EventId': eid},
                {'$set': {
                    'processed': True, 
                    'EstimatedVictimLootValue': est_value,
                    'posted_at': datetime.now(timezone.utc)
                }}
            )
            log(f"   -> Posted Event #{eid}")

        except Exception as e:
            log(f"ERROR processing {eid}: {e}")
            events_collection.update_one({'EventId': eid}, {'$set': {'processed': True, 'error': str(e)}})

# --- INITIALIZE BOT ---
bot = KillboardBot()

# --- COMMANDS ---

@bot.command()
async def event(ctx, event_id: int):
    """Manually fetch and display an event by ID for debugging."""
    await ctx.send(f"🔍 Fetching event {event_id}...")
    
    url = f'https://gameinfo-sgp.albiononline.com/api/gameinfo/events/{event_id}'
    
    async with aiohttp.ClientSession() as session:
        try:
            async with session.get(url, headers=API_HEADERS) as resp:
                if resp.status != 200:
                    await ctx.send(f"API Error: {resp.status}")
                    return               
                event_data = await resp.json()

                try:
                    event_data['CreatedAt'] = datetime.fromisoformat(event_data['TimeStamp'].replace('Z', '+00:00'))
                except:
                    event_data['CreatedAt'] = datetime.now(timezone.utc)

                est_value = await get_estimated_value(session, event_data['Victim'])
                event_data['EstimatedVictimLootValue'] = est_value
                
                img_bytes = await generate_versus_image(session, event_data)
                
                file = discord.File(img_bytes, filename="killboard.png")
                embed = create_embed(event_data, est_value)
                await ctx.send(embed=embed, file=file)
                
        except Exception as e:
            await ctx.send(f"Error: {e}")

@bot.command()
async def track(ctx, name: str):
    if len(name) < 3:
        await ctx.send("Name too short.")
        return
    try:
        tracked_collection.update_one(
            {'name': name}, 
            {'$set': {'name': name, 'added_by': ctx.author.name, 'added_at': datetime.now()}}, 
            upsert=True
        )
        await ctx.send(f"✅ Now tracking: **{name}**")
        log(f"Command: Tracked {name}")
    except Exception as e:
        await ctx.send(f"Error: {e}")

@bot.command()
async def untrack(ctx, name: str):
    result = tracked_collection.delete_one({'name': name})
    if result.deleted_count > 0:
        await ctx.send(f"🗑️ Stopped tracking: **{name}**")
        log(f"Command: Untracked {name}")
    else:
        await ctx.send(f"Player **{name}** was not found in the list.")

@bot.command(name="list")
async def list_tracked(ctx):
    players = list(tracked_collection.find({}, {'name': 1}))
    if not players:
        await ctx.send("No players are currently being tracked.")
        return
    names = [p['name'] for p in players]
    msg = "**Tracked Players:**\n" + ", ".join(names)
    await ctx.send(msg)

# --- SILVER LOGIC ---
async def get_estimated_value(session, victim_data):
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
        async with session.get(url, headers=API_HEADERS) as resp:
            if resp.status != 200: return 0
            price_data = await resp.json()
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

# --- IMAGE GENERATION ---
async def fetch_image(session, url):
    try:
        async with session.get(url, headers=API_HEADERS) as resp:
            if resp.status == 200:
                data = await resp.read()
                return Image.open(BytesIO(data)).convert("RGBA")
    except: pass
    return None

def apply_transparency(img, alpha_float):
    if img.mode != 'RGBA':
        img = img.convert('RGBA')
    r, g, b, a = img.split()
    a = a.point(lambda p: int(p * alpha_float))
    return Image.merge('RGBA', (r, g, b, a))

async def generate_versus_image(session, doc):
    # --- IMAGE CONF ---
    IMG_CONFIGS = {
        'killer_mewing': {
            'width': 180,       # Width of the image 
            'opacity': 0.6,     # 0.0 to 1.0
            'x_offset': 0,      # Move Left (-) or Right (+)
            'y_offset': 0       # Move Up (-) or Down (+)
        },
        'killer_mogs': {        # Killer's Head
            'size': 120,        # Square size (160x160)
            'opacity': 0.5,
            'x_offset': 0,      # Move Left (-) or Right (+)
            'y_offset': -30     # Move Up (-) or Down (+)
        },
        'jackass': {            # Victim's Head
            'size': 120,        # Square size (160x160)
            'opacity': 0.7,
            'x_offset': 0,      # Move Left (-) or Right (+)
            'y_offset': -19     # Move Up (-) or Down (+)
        }
    }
    # ---------------------------------------

    killer = doc['Killer']
    victim = doc['Victim']
    participants = [p for p in doc.get('Participants', []) if p['Name'] != killer['Name']]
    inventory_items = [i for i in victim.get('Inventory', []) if i is not None]
    
    # --- TRACKING CHECKS ---
    tracked_cursor = tracked_collection.find({}, {'name': 1})
    tracked_names = {d['name'].lower() for d in tracked_cursor}
    
    is_killer_tracked = killer['Name'].lower() in tracked_names
    is_victim_tracked = victim['Name'].lower() in tracked_names
    
    # Check if any participant is tracked (Assist)
    participant_names = [p['Name'].lower() for p in doc.get('Participants', [])]
    is_assist = any(name in tracked_names for name in participant_names) and not is_killer_tracked and not is_victim_tracked

    # --- ANATOMY IMAGE CONFIGURATION ---
    ANATOMY_W = 400
    ANATOMY_H = 433
    ICON_SIZE = 100 
    
    SLOT_COORDS = {
        'Bag':      (20, 28), 'Head':     (150, 37), 'Cape':     (279, 28),
        'MainHand': (44, 130), 'Armor':    (150, 130), 'OffHand':  (259, 130),
        'Food':     (279, 237), 'Potion':   (23, 237), 'Shoes':    (150, 224),
        'Mount':    (150, 318)
    }
    
    # Layout Calculations
    padding_x = 40
    header_height = 120 
    inv_icon_size = 72
    inv_gap = 5
    inv_cols = 9
    inv_rows = math.ceil(len(inventory_items) / inv_cols) if inventory_items else 0
    inv_section_height = (inv_rows * (inv_icon_size + inv_gap)) + 60 
    
    total_width = (ANATOMY_W * 2) + (padding_x * 3) 
    total_height = header_height + ANATOMY_H + 40 + inv_section_height
    
    bg_color = (190, 157, 106, 255)
    canvas = Image.new('RGBA', (total_width, total_height), bg_color)
    draw = ImageDraw.Draw(canvas)

    # --- ASSET (LOCAL) ---
    BASE_DIR = os.path.dirname(os.path.abspath(__file__))
    ASSETS_DIR = os.path.join(BASE_DIR, 'assets')
    
    def load_local_asset(filename):
        try:
            path = os.path.join(ASSETS_DIR, filename)
            if os.path.exists(path):
                return Image.open(path).convert("RGBA")
            log(f"Missing asset: {filename}")
            return None
        except Exception as e:
            log(f"Error loading {filename}: {e}")
            return None

    # Load Static Assets
    bg_image = load_local_asset("gear.png")
    silver_image = load_local_asset("bag_of_silver.png")
    
    # Determine Overlays
    overlay_img = None
    face_img = None
    
    # Headgear specific overlays
    killer_head_overlay = None
    victim_head_overlay = None

    if is_victim_tracked:
        overlay_img = load_local_asset("mogged.png")
        face_img = load_local_asset("moggedlul.png")
    elif is_killer_tracked:
        # 1. VS Overlay (Mewing)
        raw_img = load_local_asset("killer_mewing.png")
        cfg = IMG_CONFIGS['killer_mewing']
        if raw_img:
            overlay_img = apply_transparency(raw_img, cfg['opacity'])
        
        # 2. Killer Head (Mogs)
        raw_k_head = load_local_asset("killer_mogs.png")
        cfg_k = IMG_CONFIGS['killer_mogs']
        if raw_k_head:
            killer_head_overlay = apply_transparency(raw_k_head, cfg_k['opacity'])
            
        # 3. Victim Head (Jackass)
        raw_v_head = load_local_asset("jackass.png")
        cfg_v = IMG_CONFIGS['jackass']
        if raw_v_head:
            victim_head_overlay = apply_transparency(raw_v_head, cfg_v['opacity'])

    elif is_assist:
        overlay_img = load_local_asset("killer.png") 

    # --- ASSET FETCHING (NETWORK) ---
    slots = ['Bag', 'Head', 'Cape', 'MainHand', 'Armor', 'OffHand', 'Potion', 'Shoes', 'Food', 'Mount']
    tasks = [] 
    
    # 1. Killer Items
    for i, slot in enumerate(slots):
        item = killer.get('Equipment', {}).get(slot)
        if item:
            url = f"https://render.albiononline.com/v1/item/{item['Type']}.png?count={item.get('Count', 1)}&quality={item.get('Quality', 1)}"
            tasks.append(('equip', i, 'killer', fetch_image(session, url)))
    
    # 2. Victim Items
    for i, slot in enumerate(slots):
        item = victim.get('Equipment', {}).get(slot)
        if item:
            url = f"https://render.albiononline.com/v1/item/{item['Type']}.png?count={item.get('Count', 1)}&quality={item.get('Quality', 1)}"
            tasks.append(('equip', i, 'victim', fetch_image(session, url)))

    # 3. Inventory Items
    for i, item in enumerate(inventory_items):
        url = f"https://render.albiononline.com/v1/item/{item['Type']}.png?count={item.get('Count', 1)}&quality={item.get('Quality', 1)}"
        tasks.append(('inv', i, 'victim', fetch_image(session, url)))

    # Await all downloads
    results = await asyncio.gather(*[t[3] for t in tasks])
    
    # --- DRAWING LOGIC ---
    killer_x = padding_x
    victim_x = killer_x + ANATOMY_W + padding_x
    anatomy_y = header_height
    
    # 1. Paste Gear Backgrounds (From Local)
    if bg_image:
        bg_image = bg_image.resize((ANATOMY_W, ANATOMY_H))
        canvas.paste(bg_image, (killer_x, anatomy_y), bg_image)
        canvas.paste(bg_image, (victim_x, anatomy_y), bg_image)

    # 2. Text Headers
    def draw_centered_text(text, center_x, y, font, color):
        bbox = draw.textbbox((0, 0), text, font=font)
        w = bbox[2] - bbox[0]
        draw.text((center_x - (w / 2), y), text, fill=color, font=font)

    k_center = killer_x + (ANATOMY_W / 2)
    v_center = victim_x + (ANATOMY_W / 2)

    # Killer Header
    draw_centered_text("Killer", k_center, 10, BOLD_FONT, "#00000068")
    draw_centered_text(killer['Name'], k_center, 45, LARGE_BOLD_FONT, "#003366")
    if killer.get('GuildName', '') != "":
        draw_centered_text(f"[{killer.get('GuildName', '')}]", k_center, 90, SMALL_FONT, "#252525")

    # Victim Header
    draw_centered_text("Victim", v_center, 10, BOLD_FONT, "#000000")
    draw_centered_text(victim['Name'], v_center, 45, LARGE_BOLD_FONT, "#8B0000")
    if victim.get('GuildName', '') != "":
        draw_centered_text(f"[{victim.get('GuildName', '')}]", v_center, 90, SMALL_FONT, "#252525")
    
    # 3. VS Section & Overlay (Local)
    vs_x = killer_x + ANATOMY_W + (padding_x / 2)

    if overlay_img:
        m_w, m_h = overlay_img.size
        if is_killer_tracked:
            cfg = IMG_CONFIGS['killer_mewing']
            target_w = cfg['width']
            x_off = cfg['x_offset']
            y_off = cfg['y_offset']
        else:
            target_w = 120
            x_off = 0
            y_off = 0

        ratio = target_w / m_w
        overlay_img = overlay_img.resize((target_w, int(m_h * ratio)))
        mx = int(vs_x - (target_w / 2)) + x_off
        my = int(anatomy_y + (ANATOMY_H // 2) - 320) + y_off
        
        canvas.paste(overlay_img, (mx, my), overlay_img)

    draw_centered_text("VS", vs_x, anatomy_y + (ANATOMY_H // 2) - 40, LARGE_BOLD_FONT, "#FFFFFF")

    for (type_, index, side, _), img in zip(tasks, results):
        if img and type_ == 'equip':
            slot_name = slots[index]
            if slot_name in SLOT_COORDS:
                local_x, local_y = SLOT_COORDS[slot_name]
                base_x = killer_x if side == 'killer' else victim_x
                img = img.resize((ICON_SIZE, ICON_SIZE))
                canvas.paste(img, (base_x + local_x, anatomy_y + local_y), img)

    # 5. Headgear Overlays
    def paste_head_overlay(overlay_img, base_x, config):
        if not overlay_img: return
        
        target_size = config['size']
        overlay_img = overlay_img.resize((target_size, target_size))
        
        slot_x, slot_y = SLOT_COORDS['Head']
        
        slot_center_x = base_x + slot_x + (ICON_SIZE // 2)
        slot_center_y = anatomy_y + slot_y + (ICON_SIZE // 2)

        paste_x = slot_center_x - (target_size // 2)
        paste_y = slot_center_y - (target_size // 2)
        
        paste_x += config['x_offset']
        paste_y += config['y_offset']
        
        canvas.paste(overlay_img, (paste_x, paste_y), overlay_img)

    if is_killer_tracked:
        paste_head_overlay(killer_head_overlay, killer_x, IMG_CONFIGS['killer_mogs'])
        paste_head_overlay(victim_head_overlay, victim_x, IMG_CONFIGS['jackass'])

    # 6. Face Overlay
    if face_img:
        target_width = 380
        ratio = target_width / face_img.width
        target_height = int(face_img.height * ratio)
        face_img = face_img.resize((target_width, target_height))

        if face_img.mode != 'RGBA':
            face_img = face_img.convert('RGBA')
        
        r, g, b, alpha = face_img.split()
        alpha = alpha.point(lambda p: int(p * 0.6)) 
        face_img.putalpha(alpha)

        center_x = victim_x + (ANATOMY_W // 2)
        center_y = anatomy_y + (ANATOMY_H // 2)
        
        offset_up = 55
        paste_y = (center_y - (target_height // 2)) - offset_up
        paste_x = center_x - (target_width // 2)
        canvas.paste(face_img, (paste_x, paste_y), face_img)

    # 7. Stats & Silver
    stats_y = anatomy_y + 345
    stats_x = killer_x - 20
    
    part_count = len(participants)
    fame = doc.get('TotalVictimKillFame', 0)
    
    draw.text((stats_x, stats_y), f"Participants: {part_count}", fill="#252525", font=SMALL_FONT)
    draw.text((stats_x, stats_y + 22), f"Total Fame: {fame:,}", fill="#252525", font=SMALL_FONT)

    # Silver Section
    loss_y = anatomy_y + 345
    loss_x = victim_x + 310
    est_val = doc.get('EstimatedVictimLootValue', 0)
    
    if silver_image:
        silver_image = silver_image.resize((45, 45))
        canvas.paste(silver_image, (loss_x - 45, loss_y - 10), silver_image)
    
    draw.text((loss_x, loss_y), f"{est_val:,}", fill="#333333", font=SMALL_FONT)

    # Separator
    line_y = anatomy_y + ANATOMY_H + 20
    draw.line([(padding_x, line_y), (total_width - padding_x, line_y)], fill="#554433", width=3)

    # 8. Inventory Section
    if inventory_items:
        inv_start_y = line_y + 30
        grid_width = (inv_cols * inv_icon_size) + ((inv_cols - 1) * inv_gap)
        start_x = (total_width - grid_width) // 2      
        inv_results = [(t, img) for t, img in zip(tasks, results) if t[0] == 'inv']
        box_img = Image.new('RGBA', (inv_icon_size, inv_icon_size), (0, 0, 0, 60)) 
        
        total_slots = inv_rows * inv_cols
        
        for i in range(total_slots):
            col = i % inv_cols
            row = i // inv_cols
            x = start_x + (col * (inv_icon_size + inv_gap))
            y = inv_start_y + (row * (inv_icon_size + inv_gap))
            
            canvas.paste(box_img, (x, y), box_img)
            
            if i < len(inv_results):
                _, img = inv_results[i]
                if img:
                    img = img.resize((inv_icon_size, inv_icon_size))
                    canvas.paste(img, (x, y), img)
                    count = inventory_items[i].get('Count', 1)
                    if count > 1:
                        draw.text((x + 2, y + inv_icon_size - 18), str(count), fill="white", font=SMALL_FONT, stroke_width=2, stroke_fill="black")

    buffer = BytesIO()
    canvas.save(buffer, format="PNG")
    buffer.seek(0)
    return buffer

#OUTSIDEEE
def create_embed(doc, est_value):
    k = doc['Killer']
    v = doc['Victim']
    
    tracked_cursor = tracked_collection.find({}, {'name': 1})
    tracked_names = {d['name'].lower() for d in tracked_cursor}
    
    is_kill = k['Name'].lower() in tracked_names
    is_death = v['Name'].lower() in tracked_names
    tracked_participant_name = next((p['Name'] for p in doc.get('Participants', []) if p['Name'].lower() in tracked_names), None)
    
    # --- TITLE & COLOR ---
    if is_death:
        title = f"💀 DEATH: {v['Name']} was mogged by {k['Name']}"
        color = 0xff0000 
    elif is_kill:
        title = f"⚔️ KILL: {k['Name']} killed {v['Name']}"
        color = 0x00ff00 
    elif tracked_participant_name:
        # Assist Logic
        title = f"👍 ASSIST: {tracked_participant_name} assisted {k['Name']} vs {v['Name']}"
        color = 0xFFA500
    else:
        title = f"⚔️ {v['Name']} was mogged by {k['Name']}"
        color = 0x880808

    embed = discord.Embed(
        title=title,
        url=f"https://ao-kb.fly.dev/events/{doc['EventId']}",
        color=color
    )
    
    # --- PARTICIPANTS LOGIC ---
    participant_links = []
    raw_participants = doc.get('Participants', [])
    
    for p in raw_participants:
        p_name = p.get('Name')
        p_id = p.get('Id')
        
        if p_name == k['Name']:
            continue

        if p_name and p_id:
            link = f"[**{p_name}**](https://ao-kb.fly.dev/player/{p_id})"
            participant_links.append(link)

    if participant_links:
        display_limit = 6
        displayed_names = participant_links[:display_limit]
        remaining_count = len(participant_links) - display_limit
        
        participants_value = ", ".join(displayed_names)
        
        if remaining_count > 0:
            participants_value += f", and **{remaining_count} others**"
    else:
        participants_value = "Solo"

    embed.description = f"**Participants:** {participants_value}"
    
    # --- TIMESTAMP & FOOTER ---
    try:
        dt_obj = doc.get('CreatedAt')
        if isinstance(dt_obj, str):
            dt_obj = datetime.fromisoformat(dt_obj.replace('Z', '+00:00'))
        
        if not isinstance(dt_obj, datetime):
            dt_obj = datetime.now(timezone.utc)

        if dt_obj.tzinfo is None:
            dt_obj = dt_obj.replace(tzinfo=timezone.utc)

        ph_tz = timezone(timedelta(hours=8))
        dt_ph = dt_obj.astimezone(ph_tz)
        formatted_time = dt_ph.strftime('%Y-%m-%d %I:%M %p (PH)')
    except Exception as e:
        log(f"Time formatting error: {e}")
        formatted_time = doc.get('TimeStamp', 'Unknown Time')

    event_url = f"https://ao-kb.fly.dev/events/{doc['EventId']}"
    
    embed.set_footer(text=f"Event ID: {doc['EventId']} | {formatted_time}\n{event_url}")
    embed.set_image(url="attachment://killboard.png")
    
    return embed

def is_authorized():
    async def predicate(ctx):
        is_owner = ctx.author.id == 324253509610504193 
        is_admin = ctx.author.guild_permissions.administrator
        return is_owner or is_admin
    return commands.check(predicate)

@bot.command()
@is_authorized()
async def setchannel(ctx):
    """Sets the current channel as the killboard output."""
    settings_collection.update_one(
        {'_id': 'config'},
        {'$set': {'channel_id': ctx.channel.id}},
        upsert=True
    )
    global CHANNEL_ID
    CHANNEL_ID = ctx.channel.id
    await ctx.send(f"✅ Killboard channel set to: {ctx.channel.mention}")

@bot.command()
@is_authorized()
async def removechannel(ctx):
    """Removes the killboard output channel."""
    settings_collection.update_one(
        {'_id': 'config'},
        {'$unset': {'channel_id': ""}}
    )
    global CHANNEL_ID
    CHANNEL_ID = None
    await ctx.send("🗑️ Killboard channel removed.")

if __name__ == "__main__":
    if not TOKEN:
        log("Error: DISCORD_BOT_TOKEN not found.")
    else:
        bot.run(TOKEN)