import warnings                 # для того чтобы библиотеки не выдавали Предупреждение об Устаревании
warnings.filterwarnings("ignore", category=DeprecationWarning)
import pandas as pd             # Работа с DataFrame
import sqlite3 as sq            # Работа с Базой Данных
from date_base.path_to_base import DATABASE # Путь к Базе Данных
import ccxt
from datetime import datetime, date # Работа с Временем
from time import mktime
from logs import Logging

class Client:
    connections = {
        'Binance': ccxt.binance,
        'BitTeam': ccxt.bitteam,
        'ByBit': ccxt.bybit,
        'GateIo': ccxt.gateio,
        'Mexc': ccxt.mexc,
        'Okx': ccxt.okx
    }
    # Способ подключения к бирже используя getattr
    # id = 'binance'
    # exchange = getattr(ccxt, id)()
    # print(exchange.has)

    def get_client_from_DB(self):
        # active_date = date.today() + relativedelta(months=3)
        # active_timestamp = mktime(active_date.timetuple())
        timestamp = mktime(date.today().timetuple())
        exchanges_by_type = self.get_exchange_names_by_type()
        row_exchanges = '\', \''.join(exchanges_by_type)
        query = (f"""
        SELECT client, exchange, apiKey, secret, password, active_timestamp 
        FROM ApiKeys 
        WHERE client LIKE '{self.client_name}' 
            AND exchange IN ('{row_exchanges}')
            AND state IS 'Run' 
            AND active_timestamp >= {timestamp}
        """)
        with sq.connect(DATABASE) as connect:
            connect.row_factory = sq.Row  # Строки записей в виде dict {}. По умолчанию - кортежи turple ()
            curs = connect.cursor()
            curs.execute(query)
            client = curs.fetchone()
            if not client:
                return None
            # client = pd.DataFrame(columns=('client', 'exchange', 'apikey', 'secret', 'password', 'active_timestamp'))
            # for row in responce:
            #     client.loc[len(client)] = row
            # return client
            return client


    def get_exchange_names_by_type(self):
        query = (f"""
                SELECT name 
                FROM Exchanges 
                WHERE trade IS '{self.trade}'
                """)
        with sq.connect(DATABASE) as connect:
            curs = connect.cursor()
            curs.execute(query)
            return [select[0] for select in curs]




    def __init__(self, client_name, trade='Liquid_coins'):
        self.client_name = client_name
        self.trade = trade
        client_db = self.get_client_from_DB()
        if not client_db:
            Logging(f"База Данных: Не найден Аккаунт: {self.client_name} | Торговля: {self.trade}")
            return
        self.exchange = client_db['exchange']
        self.keys = self.form_keys(client_db)
        self.connection = self.connections[self.exchange](self.keys)

    def form_keys(self, client: sq.Row):
        keys = {'apiKey': client['apiKey'],
                'secret': client['secret'],
                'password': client['password']}
        return keys

if __name__ == '__main__':
    client = Client('Alehin5 Roman')
    print(client.__dict__)