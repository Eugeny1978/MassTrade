import sqlite3 as sq
import pandas as pd
from datetime import date
from dateutil.relativedelta import relativedelta
from time import mktime
from date_base.path_to_base import DATABASE

simple_tables = ('Currencies', 'States', 'TradeTypes')
currencies_data = ('1INCH', 'ADA', 'ALGO', 'ATOM', 'BTC', 'DOGE', 'DOT', 'ETH', 'FIL', 'IOTA',
                   'LINK', 'LTC', 'MATIC', 'NEAR', 'SOL', 'TRX', 'UNI', 'XLM', 'XMR', 'XRP')
states_data = ('Run', 'Pause', 'Stop')
tradetypes_data = ('Liquid_coins', 'Shit_coins')
simple_datas = {simple_tables[0]: currencies_data,
                simple_tables[1]: states_data,
                simple_tables[2]: tradetypes_data}


def create_simple_table(table_name: str):
    with sq.connect(DATABASE) as connect:
        connect.row_factory = sq.Row  # Строки записей в виде dict {}. По умолчанию - кортежи turple ()
        curs = connect.cursor()
        curs.execute(f"""CREATE TABLE IF NOT EXISTS {table_name} (
        name TEXT UNIQUE
        )""")

def create_table_Bots():
    with sq.connect(DATABASE) as connect:
        curs = connect.cursor()
        curs.execute(f"""CREATE TABLE IF NOT EXISTS Bots (
        name TEXT UNIQUE,
        state TEXT,
        FOREIGN KEY(state) REFERENCES States(name) ON DELETE SET NULL
        )""")

def create_table_Exchanges():
    with sq.connect(DATABASE) as connect:
        curs = connect.cursor()
        curs.execute(f"""CREATE TABLE IF NOT EXISTS Exchanges (
        name TEXT UNIQUE,
        trade TEXT,
        FOREIGN KEY(trade) REFERENCES TradeTypes(name) ON DELETE SET NULL
        )""")

def create_table_Clients():
    with sq.connect(DATABASE) as connect:
        curs = connect.cursor()
        curs.execute(f"""CREATE TABLE IF NOT EXISTS Clients (
        name TEXT UNIQUE,
        liquid_params TEXT UNIQUE,
        shit_params TEXT UNIQUE
        )""")

def create_table_ApiKeys():
    """
    active_date - в формате timestamps (INTEGER). Для удобной выбоки по дате (База не поддерживает тип данных Дата)
    Можно сделать доп Колонку с Датой отображения в виде текста 2024-01-26 (TEXT) - удобно просматривать человеком
    И затем при заполнении этой колонки получать значения active_date
    """
    with sq.connect(DATABASE) as connect:
        curs = connect.cursor()
        curs.execute(f"""CREATE TABLE IF NOT EXISTS ApiKeys (
        client TEXT,
        exchange TEXT,
        apiKey TEXT,
        secret TEXT,
        password TEXT,        
        active_timestamp INTEGER,
        state TEXT,
        active_date TEXT,
        FOREIGN KEY(client) REFERENCES Clients(name) ON DELETE SET NULL,
        FOREIGN KEY(exchange) REFERENCES Exchanges(name) ON DELETE SET NULL,
        FOREIGN KEY(state) REFERENCES States(name) ON DELETE SET NULL
        )""")

def get_table_names():
    with sq.connect(DATABASE) as connect:
        request_text = "SELECT name FROM sqlite_schema WHERE type='table';"
        cur = connect.cursor()
        cur.execute(request_text)
        responce = cur.fetchall()
        tables = [table[0] for table in responce]
    return tables

def drop_tables(tables: list or tuple = ()):
    if not len(tables):
        tables = get_table_names()
    with sq.connect(DATABASE) as connect:
        curs = connect.cursor()
        for table in tables:
            curs.execute(f"DROP TABLE IF EXISTS {table}")

def fill_simple_table(table: str, data: tuple or list):
    with sq.connect(DATABASE) as connect:
        curs = connect.cursor()
        for value in data:
             if check_no_value(value, 'name', table, curs):
                curs.execute(f"""INSERT INTO {table} (name) VALUES ('{value}')""")

def delete_rows(table_name: str, values: list or tuple=()):
    with sq.connect(DATABASE) as connect:
        request_text = f"DELETE FROM {table_name}"
        curs = connect.cursor()
        if len(values):
            injection = '\', \''.join(values)
            value_request_text = f"{request_text} WHERE name IS NULL OR name IN ('{injection}')"
            curs.execute(value_request_text)
        else:
            curs.execute(request_text)

def check_no_value(value, column, table, cursor):
    cursor.execute(f"SELECT {column} FROM {table} WHERE {column} IS '{value}'")
    responce = cursor.fetchone()
    no_value = False if responce else True
    return no_value

def fill_table_Exchanges():
    path_to_file = './csv/Exchanges.csv'
    df = pd.read_csv(path_to_file, delimiter=';')
    with sq.connect(DATABASE) as connect:
        curs = connect.cursor()
        for index, row in df.iterrows():
            name = row['name']
            trade = get_trade_value(name)
            if check_no_value(name, 'name', 'Exchanges', curs):
                curs.execute(f"INSERT INTO Exchanges (name, trade) VALUES ('{name}', '{trade}')")

def fill_table_Clients():
    path_to_file = './csv/ApiKeys.csv'
    df = pd.read_csv(path_to_file, delimiter=';')
    with sq.connect(DATABASE) as connect:
        curs = connect.cursor()
        for index, row in df.iterrows():
            name = row['name']
            lq_params = f'LqParams_{name}'
            sh_params = f'ShParams_{name}'
            if check_no_value(name, 'name', 'Clients', curs):
                curs.execute(f"INSERT INTO Clients (name, liquid_params, shit_params) VALUES ('{name}', '{lq_params}', '{sh_params}')")

def fill_table_ApiKeys():
    path_to_file = './csv/ApiKeys.csv'
    df = pd.read_csv(path_to_file, delimiter=';')
    table = 'ApiKeys'
    with sq.connect(DATABASE) as connect:
        curs = connect.cursor()
        for index, row in df.iterrows():
            client = row['name']
            exchange = row['exchange']
            apiKey = row['apiKey']
            secret = row['secret']
            password = row['password'] if row['password'] else None
            state = get_state_value(row['status'])
            active_date = date.today() + relativedelta(months=3)
            active_timestamp = mktime(active_date.timetuple())

            no_client = check_no_value(client, 'client', table, curs)
            no_apiKey = check_no_value(apiKey, 'apiKey', table, curs)
            no_secret = check_no_value(secret, 'secret', table, curs)
            no_password = check_no_value(password, 'password', table, curs)
            no_record = (no_client and no_apiKey and no_secret and no_password)
            if no_record:
                text = f"""INSERT INTO {table}
                (client, exchange, apiKey, secret, password, active_timestamp, state, active_date) VALUES
                ('{client}', '{exchange}', '{apiKey}', '{secret}', '{password}', {active_timestamp}, '{state}', '{active_date}')"""
                curs.execute(text)

def get_state_value(status): # ('Active', 'Passive')
    value = states_data[0] if status == 'Active' else states_data[2]
    return value

def get_trade_value(exchange: str):
    value = tradetypes_data[0]
    if exchange in ('Mexc', 'BitTeam'):
        value = tradetypes_data[1]
    return value


if __name__ == '__main__':

    for table_name in simple_tables:
        create_simple_table(table_name)

    create_table_Bots()
    create_table_Exchanges()
    create_table_Clients()
    create_table_ApiKeys()

    for table, data in simple_datas.items():
        fill_simple_table(table, data)

    fill_table_Exchanges()
    fill_table_Clients()
    fill_table_ApiKeys()

    # drop_tables(('Clients', 'ApiKeys'))
    # drop_tables()

    # delete_rows('States', ('Pause', 'Run', 'Stop'))
    # for table in simple_tables:
    #     delete_rows(table)

