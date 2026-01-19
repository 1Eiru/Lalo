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
status_collection = db.status  # New collection for syncing time

# Initialize status in DB if it doesn't exist
if status_collection.count_documents({'_id': 'main_status'}) == 0:
    status_collection.insert_one({
        '_id': 'main_status',
        'last_update': 'Waiting for first update...',
        'next_update_ts': time.time() + 90
    })

last_fetched_ids = set()

def get_status():
    """Helper to get current status from DB"""
    return status_collection.find_one({'_id': 'main_status'})

def update_status_time(next_ts=None, last_update_str=None):
    """Helper to update status in DB"""
    update_fields = {}
    if next_ts:
        update_fields['next_update_ts'] = next_ts
    if last_update_str:
        update_fields['last_update'] = last_update_str
    
    if update_fields:
        status_collection.update_one(
            {'_id': 'main_status'},
            {'$set': update_fields}
        )

def fetch_event_ids():
    try:
        response = requests.get('https://gameinfo-sgp.albiononline.com/api/gameinfo/events')
        data = response.json()
        return set(event['EventId'] for event in data)
    except Exception as e:
        print(f"Error fetching IDs: {e}")
        return set()

def fetch_event_details(event_id):
    response = requests.get(f'https://gameinfo-sgp.albiononline.com/api/gameinfo/events/{event_id}')
    data = response.json()
    return data

def fetch_and_check_events():
    global last_fetched_ids

    current_ids = fetch_event_ids()
    if not current_ids:
        return False

    new_ids = current_ids - last_fetched_ids

    if new_ids:
        for event_id in new_ids:
            try:
                event_details = fetch_event_details(event_id)
                events_collection.update_one(
                    {'EventId': event_details['EventId']},
                    {'$set': event_details},
                    upsert=True,
                )
                print(f"Successfully updated event {event_id}")
            except Exception as e:
                print(f"Error updating event {event_id}: {e}")
        
        last_fetched_ids = current_ids
        
        # Update the "Last Update" text in DB
        now_str = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        update_status_time(last_update_str=now_str)
        
        print(f"Updated {len(new_ids)} new events.")
        return True
    else:
        print("No new events found.")
        return False

@scheduler.task('interval', id='regular_check', seconds=90, misfire_grace_time=900)
def scheduled_update_event():
    with app.app_context():
        # 1. Run the update logic
        fetch_and_check_events()
        
        # 2. Set the NEXT time this will run (Current time + 90 seconds)
        # We update this in the DB so all workers/users see the new time
        next_ts = time.time() + 90
        update_status_time(next_ts=next_ts)

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
    status = get_status()
    return render_template('home.html', 
                           datas=processed_datas, 
                           last_update=status.get('last_update'),
                           next_update_ts=status.get('next_update_ts'))

@app.route("/events/<int:event_id>")
def events(event_id):
    data = events_collection.find({'EventId': event_id})
    return render_template('events.html', events=data)

@app.route('/api/updates')
def api_updates():
    latest_events = get_latest_events()
    status = get_status()
    return jsonify({
        'events': latest_events,
        'last_update': status.get('last_update'),
        'next_update_ts': status.get('next_update_ts')
    })

if __name__ == '__main__':
    app.run(threaded=True)