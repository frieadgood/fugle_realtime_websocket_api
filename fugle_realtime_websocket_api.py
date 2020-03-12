#!/usr/bin/env python
# coding: utf-8

import dash
import dash_core_components as dcc
import dash_html_components as html
import dash_table
from dash.dependencies import Input, Output, State


import pandas as pd
import numpy as np
import datetime
import websocket
import sys 
import trace 
import threading
import time 
import requests
import json


class create_Thread(threading.Thread):
    
    def __init__(self, *args, **keywords): 
        threading.Thread.__init__(self, *args, **keywords) 
        self.killed = False
  
    def start(self):
        self.__run_backup = self.run 
        self.run = self.__run       
        threading.Thread.start(self) 

    def __run(self): 
        sys.settrace(self.globaltrace) 
        self.__run_backup() 
        self.run = self.__run_backup 

    def globaltrace(self, frame, event, arg): 
        if event == 'call': 
            return self.localtrace 
        else: 
            return None

    def localtrace(self, frame, event, arg): 
        if self.killed: 
            if event == 'line': 
                raise SystemExit() 
        return self.localtrace 

    def kill(self): 
        self.killed = True



class chart_websocket_api():
    
    def __init__(self, api_token):
        
        self.api_token = api_token

    def chart_websocket(self, symbol_id):

        def on_message(ws, message):

            self.chart_msg = json.loads(message)
        
        def on_error(ws, error):
            print(error)

        def on_close(ws):
            print("### closed ###")

#         if __name__ == "__main__":
        websocket.enableTrace(False)
        ws = websocket.WebSocketApp('wss://api.fugle.tw/realtime/v0/intraday/chart?symbolId=' + symbol_id +
                                    '&apiToken=' + self.api_token,
                                    on_message=on_message,
                                    on_error=on_error,
                                    on_close=on_close)
        
        ws.run_forever()
        
    def get_chart_msg(self, symbol_id):

        try:
            symbol = self.chart_msg['data']['info']['symbolId']

            if symbol_id != symbol:

                while [i for i in threading.enumerate() if i.name == symbol+'chart'] != []:

                    [i for i in threading.enumerate() if i.name == symbol+'chart'][0].kill()

                    time.sleep(1)

                print('kill complete！')
                t = create_Thread(target=self.chart_websocket, args=[symbol_id], name=symbol_id+'chart')
                t.start()
                time.sleep(2)

            else:
                print('pass')

        except (NameError, AttributeError):
            t = create_Thread(target=self.chart_websocket, args=[symbol_id], name=symbol_id+'chart')
            t.start()
            time.sleep(2)
            print('new')
            
            
    def get_chart_data(self, n, symbol_id):

        self.get_chart_msg(symbol_id)

        now = datetime.datetime.now()
        today = now.strftime('%Y-%m-%d')
        close_time = datetime.datetime(now.year,now.month,now.day, 13, 30)

        time_index = pd.date_range(start=f'{today} 09:00:00',
                                   end=f'{today} 13:30:00', freq=f'{n}T', closed='right')

        df_time = pd.DataFrame(time_index, columns=['at'])

        ohlc = self.chart_msg['data']['chart']
        df = pd.DataFrame(ohlc.values())

        df['at'] = pd.DataFrame(ohlc.keys())
        df['at'] = pd.to_datetime(df['at']).dt.tz_localize(None) + datetime.timedelta(hours=8)
        df = df.set_index('at')

        df = df.asfreq('1T')
        df['close'] = df['close'].fillna(method='ffill')
        df['volume'] = df['volume'].fillna(0)
        df = df.fillna(axis=1, method='ffill')

        df_ohlc = df.resample(f'{n}T', kind='period').agg({'open': 'first',
                                                           'high': 'max',
                                                           'low': 'min',
                                                           'close': 'last',
                                                           'volume': 'sum'
                                                          })

        df_ohlc.index = df_ohlc.index + datetime.timedelta(minutes=n-1)

        df_ohlc = df_ohlc.to_timestamp().reset_index()
        df_ohlc = pd.merge(df_time, df_ohlc, on='at', how='outer')
        df_ohlc['at'] = df_ohlc['at'].apply(lambda x: x if x <= close_time else close_time)
        df_ohlc['at'] = df_ohlc['at'].astype(str)

        return df_ohlc
    
    
    def plot_ohlc(self, df, rise_color, down_color):
        
        df['at'] = df['at'].astype(str)
        
        return {
            'type':'candlestick',
            'x':df['at'],
            'open':df['open'],
            'high':df['high'],
            'low':df['low'],
            'close':df['close'],
            'name':'K線圖',
            'increasing':{'line':{'color':rise_color}},
            'decreasing':{'line':{'color':down_color}}
        }

    def plot_MA(self, df, n, line_color, line_width):

        df[f'{n}MA'] = df['close'].rolling(n).mean()

        return {
            'type':'scatter',
            'x':df['at'],
            'y':df[f'{n}MA'],
            'mode':'lines',
            'line':{'color':line_color, 'width':line_width},
            'name':f'{n}MA'
        }

    def plot_volume_bar(self, df, rise_color, down_color):

        color = []
        for i in range(len(df)):
            try:
                if df['close'][i] > df['open'][i]:
                    color.append(rise_color)

                elif df['close'][i] < df['open'][i]:
                    color.append(down_color)

                elif df['close'][i] == df['open'][i]:

                    if df['close'][i] > df['close'][i-1]:
                        color.append(rise_color)
                    elif df['close'][i] < df['close'][i-1]:
                        color.append(down_color)
                    elif df['close'][i] == df['close'][i-1]:
                        color.append(color[-1])

            except (IndexError, KeyError):
                color.append(rise_color)

        return {
            'type':'bar',
            'x':df['at'],
            'y':df['volume']/1000,
            'marker':{'color':color},
            'name':'volume_bar',
            'xaxis':'x',
            'yaxis':'y2'
        }


class quote_websocket_api():
    
    def __init__(self, api_token):
        
        self.api_token = api_token
        
    def quote_websocket(self, symbol_id):

        def on_message(ws, message):

            global quote_msg
            quote_msg = json.loads(message)
            #print(json.loads(message))

        def on_error(ws, error):
            print(error)

        def on_close(ws):
            print("### closed ###")

        #if __name__ == "__main__":
        websocket.enableTrace(False)
        ws = websocket.WebSocketApp('wss://api.fugle.tw/realtime/v0/intraday/quote?symbolId=' + symbol_id +
                                    '&apiToken=' + self.api_token,
                                    on_message=on_message,
                                    on_error=on_error,
                                    on_close=on_close)
        ws.run_forever()
            
    def get_quote_msg(self, symbol_id):

        try:
            symbol = quote_msg['data']['info']['symbolId']

            if symbol_id != symbol:

                while [i for i in threading.enumerate() if i.name == symbol+'quote'] != []:

                    [i for i in threading.enumerate() if i.name == symbol+'quote'][0].kill()

                    time.sleep(1)

                print('kill complete！')
                t = create_Thread(target=self.quote_websocket, args=[symbol_id], name=symbol_id+'quote')
                t.start()

            else:
                print('pass')

        except (NameError, AttributeError):
            t = create_Thread(target=self.quote_websocket, args=[symbol_id], name=symbol_id+'quote')
            t.start()
            print('new')
            
    def get_first_quote_data(self, message):
    
        symbol = message['data']['info']['symbolId']

        ask = message['data']['quote']['order']['bestAsks']
        ask.reverse()
        df_ask = pd.DataFrame(ask, columns=['price', 'unit']).rename(columns={'unit': 'ask_unit'})

        bid = message['data']['quote']['order']['bestBids']
        bid.reverse()
        df_bid = pd.DataFrame(bid, columns=['unit', 'price']).rename(columns={'unit':'bid_unit'})

        df_quote = pd.merge(df_ask, df_bid, on='price', how='outer')
        df_quote = df_quote[['bid_unit', 'price', 'ask_unit']]

        price_list = list(df_quote['price'])

        return df_quote, price_list, symbol
    
    
    def get_new_quote_data(self, message, df_quote):

        df_quote2, price_list, symbol = self.get_first_quote_data(message)

        df_quote = pd.concat([df_quote, df_quote2], axis=0).drop_duplicates(subset='price', keep='last')
        df_quote = df_quote.sort_values('price', ascending=False).reset_index(drop=True)

        return df_quote, price_list, symbol
    
    def update_quote_data(self, input_symbol):

        global df_quote, symbol

        self.get_quote_msg(input_symbol)

        try:
            if input_symbol == symbol:
                df_quote, price_list, symbol = self.get_new_quote_data(quote_msg, df_quote)
            else:
                time.sleep(1)
                df_quote, price_list, symbol = self.get_first_quote_data(quote_msg)
        except:
            time.sleep(1)
            df_quote, price_list, symbol = self.get_first_quote_data(quote_msg)

        return df_quote, price_list, symbol
    
    def plot_order_book(self, dataframe, price_list, symbol_id):

        rows = []
        for i in range(len(dataframe)):
            row = []
            for col in dataframe.columns:
                value = dataframe.iloc[i][col]

                if col == 'price':

                    if value not in price_list:

                        cell = html.Td(html.A(href='https://www.fugle.tw/ai/'+symbol_id, children=value,style={'color':'gray'}),
                                       style={'font-size':16,'text-align':'center'})

                    elif value in price_list:

                        cell = html.Td(html.A(href='https://www.fugle.tw/ai/'+symbol_id, children=value),
                                       style={'font-size':16,'text-align':'center'})

                else:
                    cell = html.Td(children=value,style={'font-size':16,'text-align':'center'})

                row.append(cell)
            rows.append(html.Tr(row))

        return html.Table(
            [html.Tr([html.Th(col) for col in dataframe.columns],
                     style={'font-size':16,'text-align':'center', 'table-align':'center'})] +rows
        )





