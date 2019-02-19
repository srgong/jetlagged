from flask import render_template
from flask import g
from flask import request
import redis
from datetime import datetime
from jetlagged import app

def init_db():
    db = redis.StrictRedis(
    host="ec2-3-86-129-28.compute-1.amazonaws.com",
    port=6379,
    db=0)
    return db

@app.before_request
def before_request():
    g.db = init_db()

@app.context_processor
def inject_now():
    return {'now': datetime.utcnow().strftime("%s")}

@app.route('/')
@app.route('/index')
def flights_input():
    return render_template("index.html", args = feeling_adventurous())

def feeling_adventurous():
    key = g.db.randomkey()
    args = dict(arg.split('=') for arg in key.split('@'))
    return args

def convert_sec(sec):
    sec = int(sec)
    seconds=(sec)%60
    seconds = int(seconds)
    minutes=(sec/(60))%60
    minutes = int(minutes)
    hours=(sec/(60*60))%24

    print ("%d:%d:%d" % (hours, minutes, seconds))
    if hours > 0:
        time_ago = ("%d hrs %d mins %d secs ago" % (hours, minutes, seconds))
    elif minutes > 0:
        time_ago = ("%d mins %d secs ago" % (minutes, seconds))
    else:
        time_ago = ("%d secs ago" % (seconds))

    return time_ago

@app.route('/output')
def flights_output():
    adventures = feeling_adventurous()
    dest = request.args.get('to')
    origin = request.args.get('from')
    date = request.args.get('date')
    args = {'to': dest, 'from': origin, 'date': date}
    getkey = 'date='+date+'@from='+origin+"@to="+dest
    fields = g.db.hgetall(getkey)
    flights = []
    for field in fields:
        key = field.split('=')
        value = fields[field].split('@')
        field_key = {key[0]: key[1]}
        value_key = dict(v.split('=') for v in value)
        flight = dict(field_key.items() + value_key.items())
        flight['freshness'] = convert_sec(int(datetime.utcnow().strftime("%s")) - int(flight['processed_ms']))
        flights.append(flight)
        print flight
    sorted_flights = sorted(flights, key=lambda k: k['fare'])
    bests = sorted_flights.pop(0)
    if any(f['last_leg'] == 'None' for f in flights):
        bests['direct_fare'] = min(d['fare'] for d in flights if d['last_leg'] == 'None')
    else:
        bests['direct_fare'] = ""
    return render_template("output.html", args = args, bests = [bests], adventures = adventures, flights = sorted_flights, animation_time="30")
