from flask import Flask, render_template, url_for, jsonify
from pymongo import MongoClient
from datetime import datetime, timedelta
from flask_apscheduler import APScheduler
import requests, json, os, time

app = Flask(__name__)
scheduler = APScheduler()
scheduler.init_app(app)
scheduler.start()

mongoURI = os.getenv('MONGODB_URI')
client = MongoClient(mongoURI)
db = client.flask_database
events_collection = db.events
settings_collection = db.settings  # New collection for timer state

# Cache for Event IDs to prevent re-fetching details we already have
last_fetched_ids = set()

def load_existing_ids():
    """Load existing EventIds from DB on startup to avoid API spam."""
    global last_fetched_ids
    try:
        # Get all EventIds currently in our database
        existing = events_collection.find({}, {'EventId': 1})
        last_fetched_ids = set(doc['EventId'] for doc in existing)
        print(f"Loaded {len(last_fetched_ids)} existing events from DB.")
    except Exception as e:
        print(f"Error loading existing IDs: {e}")

# Load IDs immediately on startup
load_existing_ids()

def get_scheduler_status():
    """Read the timer status from MongoDB."""
    status = settings_collection.find_one({'_id': 'scheduler_status'})
    if not status:
        # Default if not found (run immediately)
        return {
            'last_check': 'Waiting for first run...',
            'next_run': time.time() 
        }
    return status

def update_scheduler_status(last_check_time):
    """Save the timer status to MongoDB."""
    # Set next run to 90 seconds from NOW
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

    # Only process IDs we haven't seen before
    new_ids = current_ids - last_fetched_ids

    if new_ids:
        print(f"Found {len(new_ids)} new events. Fetching details...")
        for event_id in new_ids:
            event_details = fetch_event_details(event_id)
            if event_details:
                try:
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
            'VictimName': event['Victim']['Name']
        }
        processed_datas.append(processed_event)
    return processed_datas

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
    
    # Get status from DB
    status = get_scheduler_status()
    
    return render_template('home.html', 
                           datas=processed_datas, 
                           last_update=status.get('last_check'),
                           next_update_ts=status.get('next_run'))

@app.route("/events/<int:event_id>")
def events(event_id):
    data = events_collection.find({'EventId': event_id})
    return render_template('events.html', events=data)

@app.route('/api/updates')
def api_updates():
    latest_events = get_latest_events()
    status = get_scheduler_status()
    return jsonify({
        'events': latest_events,
        'last_update': status.get('last_check'),
        'next_update_ts': status.get('next_run')
    })

if __name__ == '__main__':
    app.run(threaded=True)