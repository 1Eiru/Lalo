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

    async def setup_hook(self):
        self.ingest_events.start()
        self.process_events_queue.start()
        log("Ingestion and Processing loops started.")

    async def on_ready(self):
        log(f'Logged in as {self.user}')

# --- TASK 1: DEEP INGESTION [60s) ---
    @tasks.loop(seconds=60)
    async def ingest_events(self):
        await self.wait_until_ready()
        log("🔄 Starting Deep Scan Cycle...")
        
        tracked_cursor = tracked_collection.find({}, {'name': 1})
        tracked_names = {doc['name'].lower() for doc in tracked_cursor}
        
        log(f"📋 Tracking {len(tracked_names)} players.")
        
        if not tracked_names:
            log("⚠️ No players to track. Skipping API calls.")
            return 

        async with aiohttp.ClientSession() as session:
            for offset in range(0, 1001, 51):
                try:
                    url = f'https://gameinfo-sgp.albiononline.com/api/gameinfo/events?offset={offset}&limit=51'
                    log(f"🔎 Scanning Offset {offset}...") 
                    
                    async with session.get(url) as resp:
                        if resp.status != 200:
                            log(f"❌ API Error {resp.status} at offset {offset}")
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
                                    log(f"   -> 🎯 Found NEW Tracked Event! ID: {eid} ({k_name} vs {v_name})")
                                    
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
                                log(f"📥 Saved {len(events_to_save)} events to DB.")
                            except Exception as e:
                                log(f"   -> Insert warning: {e}")

                except Exception as e:
                    log(f"❌ Ingestion Exception at offset {offset}: {e}")
                    break
                
                await asyncio.sleep(0.8)

        current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        next_run = datetime.now().timestamp() + 60
        settings_collection.update_one(
            {'_id': 'scheduler_status'},
            {'$set': {'last_check': current_time, 'next_run': next_run}},
            upsert=True
        )
        log("✅ Scan Complete.")


# --- TASK 2: PROCESSING ---
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

        log(f"⚙️ Processing Queue: {len(queue)} pending events...")
        
        channel = self.get_channel(CHANNEL_ID)
        if not channel: 
            log(f"❌ Error: Discord Channel ID {CHANNEL_ID} not found or bot lacks access.")
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
            log(f"   -> ✅ Posted Event #{eid}")

        except Exception as e:
            log(f"❌ ERROR processing {eid}: {e}")
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
            async with session.get(url) as resp:
                if resp.status != 200:
                    await ctx.send(f"❌ API Error: {resp.status}")
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
            await ctx.send(f"❌ Error: {e}")

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
        async with session.get(url) as resp:
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
        async with session.get(url) as resp:
            if resp.status == 200:
                data = await resp.read()
                return Image.open(BytesIO(data)).convert("RGBA")
    except: pass
    return None

async def generate_versus_image(session, doc):
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
    
    # Coordinates
    SLOT_COORDS = {
        'Bag':      (20, 28),    
        'Head':     (150, 37),   
        'Cape':     (279, 28),   
        'MainHand': (44, 130),   
        'Armor':    (150, 130),  
        'OffHand':  (259, 130),  
        'Food':     (279, 237),  
        'Potion':   (23, 237),   
        'Shoes':    (150, 224),  
        'Mount':    (150, 318)   
    }
    
    # Layout Calculations
    padding_x = 40
    header_height = 120 
    
    # Inventory Grid
    inv_icon_size = 72
    inv_gap = 5
    inv_cols = 9
    inv_rows = math.ceil(len(inventory_items) / inv_cols) if inventory_items else 0
    inv_section_height = (inv_rows * (inv_icon_size + inv_gap)) + 60 
    
    # Total Canvas Size
    total_width = (ANATOMY_W * 2) + (padding_x * 3) 
    total_height = header_height + ANATOMY_H + 40 + inv_section_height
    
    # Create Canvas - Light Beige Background
    bg_color = (190, 157, 106, 255)
    canvas = Image.new('RGBA', (total_width, total_height), bg_color)
    draw = ImageDraw.Draw(canvas)
    
    # --- FETCH ASSETS ---
    bg_url = "https://lalokbimages.b-cdn.net/gear.png"
    silver_url = "https://lalokbimages.b-cdn.net/bag_of_silver.png"
    
    # Overlay Images
    mogged_url = "https://lalokbimages.b-cdn.net/mogged.png"
    killer_overlay_url = "https://lalokbimages.b-cdn.net/70c920445f57f6c13cb19fb606789d91.png"
    mogged_lul_url = "https://lalokbimages.b-cdn.net/moggedlul.png"
    
    slots = ['Bag', 'Head', 'Cape', 'MainHand', 'Armor', 'OffHand', 'Potion', 'Shoes', 'Food', 'Mount']
    tasks = []
    
    # 0. Backgrounds & Extras
    tasks.append(('bg', 0, 'shared', fetch_image(session, bg_url)))
    tasks.append(('icon', 0, 'silver', fetch_image(session, silver_url)))
    
    # --- OVERLAY LOGIC ---
    # Priority: Victim Tracked (Mogged) > Killer Tracked (Win) > Assist (Win)
    if is_victim_tracked:
        tasks.append(('icon', 0, 'overlay', fetch_image(session, mogged_url)))
        # face overlay task if victim is tracked
        tasks.append(('icon', 0, 'victim_face', fetch_image(session, mogged_lul_url)))
    elif is_killer_tracked or is_assist:
        tasks.append(('icon', 0, 'overlay', fetch_image(session, killer_overlay_url)))

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
    # Positioning
    killer_x = padding_x
    victim_x = killer_x + ANATOMY_W + padding_x
    anatomy_y = header_height
    
    # 1. Paste Gear Backgrounds
    bg_image = next((img for (t, _, _, _), img in zip(tasks, results) if t == 'bg'), None)
    if bg_image:
        bg_image = bg_image.resize((ANATOMY_W, ANATOMY_H))
        canvas.paste(bg_image, (killer_x, anatomy_y), bg_image)
        canvas.paste(bg_image, (victim_x, anatomy_y), bg_image)

    # 2. Text Headers (Centered)
    def draw_centered_text(text, center_x, y, font, color):
        bbox = draw.textbbox((0, 0), text, font=font)
        w = bbox[2] - bbox[0]
        draw.text((center_x - (w / 2), y), text, fill=color, font=font)

    k_center = killer_x + (ANATOMY_W / 2)
    v_center = victim_x + (ANATOMY_W / 2)

    # Killer Header - Dark Blue Name
    draw_centered_text("Killer", k_center, 10, BOLD_FONT, "#00000068")
    draw_centered_text(killer['Name'], k_center, 45, LARGE_BOLD_FONT, "#003366") 
    draw_centered_text(f"[{killer.get('GuildName', '')}]", k_center, 90, SMALL_FONT, "#252525")

    # Victim Header - Dark Red Name
    draw_centered_text("Victim", v_center, 10, BOLD_FONT, "#000000")
    draw_centered_text(victim['Name'], v_center, 45, LARGE_BOLD_FONT, "#8B0000")
    draw_centered_text(f"[{victim.get('GuildName', '')}]", v_center, 90, SMALL_FONT, "#252525")
    
    # VS Text
    vs_x = killer_x + ANATOMY_W + (padding_x / 2)
    draw_centered_text("VS", vs_x, anatomy_y + (ANATOMY_H // 2) - 40, LARGE_BOLD_FONT, "#FFFFFF")

# --- 1. CALCULATE CENTER X ---
    vs_x = killer_x + ANATOMY_W + (padding_x / 2)

    # --- 2. PASTE OVERLAY IMAGE FIRST ---
    overlay_img = next((img for (t, _, s, _), img in zip(tasks, results) if s == 'overlay'), None)
    if overlay_img:
        m_w, m_h = overlay_img.size
        target_w = 120 
        ratio = target_w / m_w
        overlay_img = overlay_img.resize((target_w, int(m_h * ratio)))
        
        mx = int(vs_x - (target_w / 2))
        my = int(anatomy_y + (ANATOMY_H // 2) - 320) 
        canvas.paste(overlay_img, (mx, my), overlay_img)

    # --- 3. DRAW "VS" TEXT LAST ---
    draw_centered_text("VS", vs_x, anatomy_y + (ANATOMY_H // 2) - 40, LARGE_BOLD_FONT, "#FFFFFF")

    # 3. Paste Equipment Icons
    for (type_, index, side, _), img in zip(tasks, results):
        if img and type_ == 'equip':
            slot_name = slots[index]
            if slot_name in SLOT_COORDS:
                local_x, local_y = SLOT_COORDS[slot_name]
                base_x = killer_x if side == 'killer' else victim_x
                img = img.resize((ICON_SIZE, ICON_SIZE))
                canvas.paste(img, (base_x + local_x, anatomy_y + local_y), img)

    # --- NEW: PASTE VICTIM FACE OVERLAY ---
    face_img = next((img for (t, _, s, _), img in zip(tasks, results) if s == 'victim_face'), None)
    if face_img:
        # Resize to fit the panel width
        target_width = 380
        ratio = target_width / face_img.width
        target_height = int(face_img.height * ratio)
        face_img = face_img.resize((target_width, target_height))

        # Make semi-transparen
        if face_img.mode != 'RGBA':
            face_img = face_img.convert('RGBA')
        
        r, g, b, alpha = face_img.split()
        alpha = alpha.point(lambda p: int(p * 0.6)) 
        face_img.putalpha(alpha)

        # Center on Victim Panel
        center_x = victim_x + (ANATOMY_W // 2)
        center_y = anatomy_y + (ANATOMY_H // 2)
        
        # Increase to go higher, decrease to go lower.
        offset_up = 55
        paste_y = (center_y - (target_height // 2)) - offset_up
        paste_x = center_x - (target_width // 2)
        canvas.paste(face_img, (paste_x, paste_y), face_img)

    # 4. Stats (Participants & Silver)
    
    # Killer Side: Participants & Fame 
    stats_y = anatomy_y + 345
    stats_x = killer_x - 20
    
    part_count = len(participants)
    fame = doc.get('TotalVictimKillFame', 0)
    
    draw.text((stats_x, stats_y), f"Participants: {part_count}", fill="#252525", font=SMALL_FONT)
    draw.text((stats_x, stats_y + 22), f"Total Fame: {fame:,}", fill="#252525", font=SMALL_FONT)

    # Victim Side: Silver
    loss_y = anatomy_y + 345
    loss_x = victim_x + 310
    
    est_val = doc.get('EstimatedVictimLootValue', 0)
    
    # Silver Icon
    silver_img = next((img for (t, _, s, _), img in zip(tasks, results) if s == 'silver'), None)
    if silver_img:
        silver_img = silver_img.resize((45, 45))
        canvas.paste(silver_img, (loss_x - 45, loss_y - 10), silver_img)
    
    draw.text((loss_x, loss_y), f"{est_val:,}", fill="#333333", font=SMALL_FONT)

    # --- SEPARATOR LINE ---
    line_y = anatomy_y + ANATOMY_H + 20
    draw.line([(padding_x, line_y), (total_width - padding_x, line_y)], fill="#554433", width=3)

    # --- INVENTORY SECTION ---
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

def create_embed(doc, est_value):
    k = doc['Killer']
    v = doc['Victim']
    
    tracked_cursor = tracked_collection.find({}, {'name': 1})
    tracked_names = {d['name'].lower() for d in tracked_cursor}
    
    is_kill = k['Name'].lower() in tracked_names
    is_death = v['Name'].lower() in tracked_names
    
    # Find if a tracked player is a participant (but not killer/victim)
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
        url=f"https://lalo-kb.onrender.com/events/{doc['EventId']}",
        color=color
    )
    
    # --- PARTICIPANTS LOGIC ---
    participant_links = []
    raw_participants = doc.get('Participants', [])
    
    for p in raw_participants:
        p_name = p.get('Name')
        p_id = p.get('Id')
        
        # FILTER: Exclude the Killer from the participants list
        if p_name == k['Name']:
            continue

        if p_name and p_id:
            # Bolded name with hyperlink
            link = f"[**{p_name}**](https://lalo-kb.onrender.com/player/{p_id})"
            participant_links.append(link)

    if participant_links:
        # Limit to the first 6 participants to keep the message clean
        display_limit = 6
        displayed_names = participant_links[:display_limit]
        remaining_count = len(participant_links) - display_limit
        
        participants_value = ", ".join(displayed_names)
        
        if remaining_count > 0:
            participants_value += f", and **{remaining_count} others**"
    else:
        participants_value = "Solo"

    # Keeps label and names on the same line
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

    event_url = f"https://lalo-kb.onrender.com/events/{doc['EventId']}"
    
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