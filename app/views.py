from flask import render_template, request, redirect, session, flash

from app import app
import app.spark as s
import pymongo, json
from pymongo import MongoClient
from datetime import date
from kafka import KafkaProducer


cluster = MongoClient("mongodb+srv://ahmedyass:webmining@cluster0-elggs.mongodb.net/test?retryWrites=true&w=majority")
database = cluster["music"]
collection = database["singles"]
#collect = database["views"]
users = database["users"]
ratings = database["ratings"]
recommendation = database["recommandations"]


@app.route('/')
def index():
    songs = collection.find({})
    if not session.get('logged_in'):
        return render_template("index-2.html", songs=songs, login=session.get('logged_in'))
    else:
        u = users.find_one({'username' : str(session.get('username'))})
        recomm = recommendation.find({"id_user":u["id"]})
        tan = []
        for kada in recomm:
            tan = kada['recommendations']
        sing = [t['id_song'] for t in tan ]
        names = collection.find({'id':{'$in':sing}})
        return render_template("index-1.html", songs=songs, names=names, recomm=tan, login=session.get('logged_in'))

@app.route('/rate', methods=['GET', 'POST'])
def rate():#matensach rate
    try:
        s.rate()
        rating = request.args.get('rate')
        kada = request.args.get('id_song')
        u = users.find_one({'username' : str(session.get('username'))})
        today = date.today()
        Date = today.strftime('%m/%d/%Y %I:%M %p')
        ratings.insert({'id_user':u['id'],'id_song':kada,'date':Date,'rating':rating})
        print(rating+"--------"+kada)
        return redirect('/')
    except:
        return "There was a problem adding new stuff."

@app.route('/login', methods=['POST'])
def do_admin_login():
    login_user = users.find_one({'username' : request.form['username']})
    
    if request.form['password'] == login_user['password'] and request.form['username'] == login_user['username']:
        session['logged_in'] = True
        session['username'] = request.form['username']
        return redirect('/')
    else:
        flash('wrong password!')
    return index()


@app.route('/fav')
def fav():
    if not session.get('logged_in'):
        return redirect('/')
    else:
        u = users.find_one({'username' : str(session.get('username'))})
        kadaRate = ratings.find({"id_user":u["id"]}).sort([ ("rating" , pymongo.DESCENDING)] ).limit(15)
        return render_template("fav.html", kadaRate=kadaRate, login=session.get('logged_in'))

@app.route("/logout")
def logout():
    session['logged_in'] = False
    return index()