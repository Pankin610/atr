#!/usr/bin/env python3
import json
import time
from collections import defaultdict
from html import escape
from uuid import uuid4
from data_client import *
import json
from atr import *

from flask import Flask, Request, request
from flask_socketio import SocketIO, join_room, leave_room
from loguru import logger
from scanner import *

app = Flask(__name__)
app.config["SECRET_KEY"] = "secret!"
socketio = SocketIO(app, cors_allowed_origins=["http://localhost:3000"])
subscribed_topics = defaultdict(set)
stocks = get_all_stocks()
print(stocks)

stock_update_topics = []
stocks_per_topic = {}

scanner_func_topics = []

atr_topics = []

scanner = None


def get_sid(request: Request) -> str:
    return getattr(request, "sid")


@app.route("/")
def index():
    json_dict = {
        sid: list(topics)
        for (sid, topics) in subscribed_topics.items()
    }
    return f"<pre>{escape(json.dumps(json_dict, indent=2))}</pre>"


@socketio.on("connect")
def on_connect():
    sid = get_sid(request)
    logger.success(f"{sid} - Connected successfully")


@socketio.on("disconnect")
def on_disconnect():
    sid = get_sid(request)
    for topic_id in subscribed_topics[sid]:
        leave_room(topic_id)
    subscribed_topics.pop(sid, None)
    logger.info(f"{sid} - Disconnected")


def check_for_stocks(topic_id):
    found_stocks = []
    for stock in stocks:
        if stock in topic_id:
            found_stocks.append(stock)
    if found_stocks:
        stock_update_topics.append(topic_id)
        stocks_per_topic[topic_id] = found_stocks
        for stock in found_stocks:
            if not stock in scanner.all_stocks:
                scanner.all_stocks.append(stock)

@socketio.on("subscribe")
def on_subscribe(topic_id):
    sid = get_sid(request)
    subscribed_topics[sid].add(topic_id)
    join_room(topic_id)
    logger.info(f"{sid} - Subscribed to {topic_id}")

    if topic_id[:4] == "atr#":
        atr_topics.append(topic_id[4:])
        return

    if topic_id == "gainers":
        scanner_func_topics.append((topic_id, scanner.get_rising_stocks))
    if topic_id == "losers":
        scanner_func_topics.append((topic_id, scanner.get_falling_stocks))

    check_for_stocks(topic_id)


@socketio.on("unsubscribe")
def on_unsubscribe(topic_id):
    sid = get_sid(request)
    subscribed_topics[sid].remove(topic_id)
    stock_update_topics.remove(topic_id)
    del stocks_per_topic[topic_id]

    leave_room(topic_id)
    logger.info(f"{sid} - Unsubscribed from {topic_id}")


def send_stock_updates():
    while True:
        for topic_id in stock_update_topics:
            for stock in stocks_per_topic[topic_id]:
                if not stock in scanner.stock_data:
                    continue
                payload = {
                    "topicId": topic_id,
                    "stock": stock,
                    "recent_bars": [bar.to_dict() for bar in scanner.stock_data[stock]]
                }
                socketio.emit("message", payload, room=topic_id)
        time.sleep(10)

def send_scanner_endpoints():
    while True:
        for (topic_id, func) in scanner_func_topics:
            stocks = func()
            payload = {"topicId": topic_id, "detected_stocks": stocks}
            socketio.emit("message", payload, room=topic_id)
        time.sleep(10)


def parse_atr_params(req):
    instrument = ""
    timeframe = ""
    for param in req.split("$"):
        name, val = param.split("=")[:2]
        if name == "instrument":
            instrument = val
        if name == "timeframe":
            timeframe = val
        
    return instrument, timeframe


def send_atr():
    while True:
        for topic_id in atr_topics:
            instrument, timeframe = parse_atr_params(topic_id)
            try:
                atr = get_current_atr(instrument, timeframe, scanner.data_client)
            except Exception:
                print("ATR unavailable")
            
            payload = {"topicId": topic_id, "atr": str(atr)}
            socketio.emit("message", payload, room=topic_id)
        time.sleep(10)


if __name__ == "__main__":
    scanner = Scanner(MixedDataClient())
    socketio.start_background_task(send_stock_updates)
    socketio.start_background_task(send_scanner_endpoints)
    socketio.start_background_task(send_atr)
    socketio.run(app, port=5001)