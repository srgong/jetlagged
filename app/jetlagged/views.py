from flask import render_template
from flask import g
from flask import request
import redis
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

@app.route('/')
@app.route('/input')
def flights_input():
    # LAX@SLC@2017-12-01T06:30:00.000-05:00
    keys = g.db.keys("*")
    flights = []
    # a = [ 'abc=lalalla', 'appa=kdkdkdkd', 'kkakaka=oeoeoeo']
    # flights = dict(s.split('@') for s in keys)
    # print d
    # for flight in flights:
        # print flight
    for key in keys:
        print key
        flights.append(key.split("@"))
        # dict(key.split('@')
    return render_template("output.html", flights = flights)

@app.route('/output')
def flights_output():
    dest = request.args.get('to')
    origin = request.args.get('from')
    date = request.args.get('date')
    args = {'to': dest, 'from': origin, 'date': date}
    fare = g.db.get(origin+"@"+dest+"@"+date)
    # fare = g.db.hgetall(origin+"@"+dest+"@"+date)
    flights = {'key': origin+'->'+dest,'last_leg': 'JFK','fare': fare,'date': date}
    return render_template("output.html", args = args, flights = [flights])
