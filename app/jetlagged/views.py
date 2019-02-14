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
    flights = []
    return render_template("index.html")

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
        fieldKey = {key[0]: key[1]}
        valueKey = dict(v.split('=') for v in value)
        flight = dict(fieldKey.items() + valueKey.items())
        flight['freshness'] = convert_sec(int(datetime.utcnow().strftime("%s")) - int(flight['processed_ms']))
        flights.append(flight)
        print flight
    return render_template("output.html", args = args, flights = flights, animation_time="30")
