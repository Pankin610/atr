from flask import Flask
from flask_restful import Resource, Api, reqparse
import pandas as pd
import yfinance as yf
from datetime import datetime

app = Flask(__name__)
api = Api(app)

parser = reqparse.RequestParser()
        
parser.add_argument('ticker', type=str)
parser.add_argument('start', type=str)
parser.add_argument('end', type=str)
parser.add_argument('interval', type=str)

class History(Resource):
    def post(self):
        
        args = parser.parse_args()
        print(args)
        ticker = args['ticker']
        start = args['start']
        end = args['end']
        interval = args['interval']

        start = datetime.strptime(str(start), "%Y-%m-%d")
        end = datetime.strptime(str(end), "%Y-%m-%d")

        data = yf.download(
            ticker,
            start=start,
            end=end,
            interval=interval,
            progress=False,
            show_errors=True,
            ignore_tz=True,
        )

        return data.to_json(), 200  # return data with 200 OK

api.add_resource(History, '/history')

if __name__ == '__main__':
    app.run()