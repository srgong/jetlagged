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
        key = field.split('@')
        value = fields[field].split('@')
        fieldKey = dict(k.split('=') for k in key)
        valueKey = dict(v.split('=') for v in value)
        flight = dict(fieldKey.items() + valueKey.items())
        flight['freshness'] = ( int(datetime.utcnow().strftime("%s")) - int(flight['processed_ms']) )
        flights.append(flight)
        print flight
    return render_template("output.html", args = args, flights = flights, animation_time="30")
