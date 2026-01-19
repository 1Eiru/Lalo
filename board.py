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

# Global variables
last_update_time = "Waiting for first update..."
last_fetched_ids = set()
next_update_timestamp = time.time() + 90 

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
    global last_update_time, last_fetched_ids

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
        last_update_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        print(f"Updated {len(new_ids)} new events.")
        return True
    else:
        print("No new events found.")
        return False

@scheduler.task('interval', id='regular_check', seconds=90, misfire_grace_time=900)
def scheduled_update_event():
    global next_update_timestamp
    
    with app.app_context():
        # update logic
        fetch_and_check_events()
        next_update_timestamp = time.time() + 90

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

    return render_template('home.html', 
                           datas=processed_datas, 
                           last_update=last_update_time,
                           next_update_ts=next_update_timestamp)

@app.route("/events/<int:event_id>")
def events(event_id):
    data = events_collection.find({'EventId': event_id})
    return render_template('events.html', events=data)

@app.route('/api/updates')
def api_updates():
    latest_events = get_latest_events()
    return jsonify({
        'events': latest_events,
        'last_update': last_update_time,
        'next_update_ts': next_update_timestamp # Send the sync time to client
    })

if __name__ == '__main__':
    app.run(threaded=True)