import sqlite3 as sq
import pandas as pd
from datetime import date
from dateutil.relativedelta import relativedelta
from time import mktime
from data_base.path_to_base import DATABASE

pd.options.display.width= None # Отображение Таблицы на весь Экран
pd.options.display.max_columns= 20 # Макс Кол-во Отображаемых Колонок
pd.options.display.max_rows= 100 # Макс Кол-во Отображаемых Строк

simple_tables = ('Currencies', 'States', 'TradeTypes')
currencies_data = ('1INCH', 'ADA', 'ALGO', 'ATOM', 'BTC', 'DOGE', 'DOT', 'ETH', 'FIL', 'IOTA',
                   'LINK', 'LTC', 'MATIC', 'NEAR', 'SOL', 'TRX', 'UNI', 'XLM', 'XMR', 'XRP')
states_data = ('Run', 'Pause', 'Stop')
tradetypes_data = ('Liquid_coins', 'Shit_coins')
simple_datas = {simple_tables[0]: currencies_data,
                simple_tables[1]: states_data,
                simple_tables[2]: tradetypes_data}
liquid_currencies = ('ATOM', 'BTC', 'ETH')
shit_currencies = ('DEL', )


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
    active_timestamp - в формате timestamps (INTEGER). Для удобной выбоpки по дате (База не поддерживает тип данных Дата)
    active_date - с Датой отображения в виде текста 2024-01-26 (TEXT) - удобно просматривать человеком
    И затем при заполнении этой колонки получать значения active_timestamp
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

def create_table_Params(table_name):
    with sq.connect(DATABASE) as connect:
        curs = connect.cursor()
        curs.execute(f"""CREATE TABLE IF NOT EXISTS {table_name} (
        currency TEXT,
        stake INTEGER DEFAULT 11,
        delta_buy INTEGER DEFAULT 3, 
        delta_sell INTEGER DEFAULT 9,
        max_num INTEGER DEFAULT 10,
        number INTEGER DEFAULT 0,
        FOREIGN KEY(currency) REFERENCES Currencies(name)
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

def import_records() -> pd.DataFrame:
    path_to_file1 = './csv/Clients.csv'
    path_to_file2 = './csv/Clients_Mexc.csv'
    df1 = pd.read_csv(path_to_file1, delimiter=';')
    df2 = pd.read_csv(path_to_file2, delimiter=';')
    df = pd.concat([df1, df2], ignore_index=True).sort_values(by=['name', 'exchange']).reset_index(drop=True) # ascending=False если по убыванию
    return df

def fill_table_Clients():
    df =import_records()
    with sq.connect(DATABASE) as connect:
        curs = connect.cursor()
        for index, row in df.iterrows():
            name = row['name']
            adaptive_name = name.replace(' ', '_')
            lq_params = f'LqParams_{adaptive_name}'
            sh_params = f'ShParams_{adaptive_name}'
            if check_no_value(name, 'name', 'Clients', curs):
                curs.execute(f"INSERT INTO Clients (name, liquid_params, shit_params) VALUES ('{name}', '{lq_params}', '{sh_params}')")

def adapt_client_name(name):
    return name.replace(' ', '_')

def check_no_value(value, column, table, cursor):
    cursor.execute(f"SELECT {column} FROM {table} WHERE {column} IS '{value}'")
    responce = cursor.fetchone()
    no_value = False if responce else True
    return no_value

def check_no_client_and_exchange(client, exchange, cursor):
    cursor.execute(f"SELECT client FROM ApiKeys WHERE client IS '{client}' AND  exchange IS '{exchange}'")
    responce = cursor.fetchone()
    no_value = False if responce else True
    return no_value

def fill_table_ApiKeys():
    df = import_records()
    table = 'ApiKeys'
    with sq.connect(DATABASE) as connect:
        curs = connect.cursor()
        for index, row in df.iterrows():
            client, exchange = row['name'], row['exchange']
            apiKey, secret, password = row['apiKey'], row['secret'], row['password']
            state = get_state_value(row['status'])
            active_date = date.today() + relativedelta(months=3)
            active_timestamp = mktime(active_date.timetuple())

            no_record = check_no_client_and_exchange(client, exchange, curs)
            if no_record:
                text = f"""INSERT INTO {table}
                (client, exchange, apiKey, secret, active_timestamp, state, active_date, password) VALUES
                ('{client}', '{exchange}', '{apiKey}', '{secret}', {active_timestamp}, '{state}', '{active_date}', """
                if pd.isna(password):
                    password_text = f" NULL)"
                else:
                    password_text = f"'{password}')"
                curs.execute(text+password_text)

def fill_all_tables_Params():
    with sq.connect(DATABASE) as connect:
        connect.row_factory = sq.Row  # Строки записей в виде dict {}. По умолчанию - кортежи turple ()
        curs = connect.cursor()
        clients = get_clients_for_table_Params(curs)
        for client in clients:
            client_name = adapt_client_name(client['client'])
            if client['exchange'] in ('Mexc', 'BitTeam'):
                currencies = shit_currencies
                trade_name = "ShParams_"
            else:
                currencies = liquid_currencies
                trade_name = "LqParams_"
            table_name = trade_name + client_name
            fill_table_Params(table_name, currencies, curs)

def fill_table_Params(table: str, currencies: list or tuple, cursor):
    text_values = ''
    for c in currencies:
        text_values += f"('{c}'), "
    text_values = text_values.rstrip(', ')
    cursor.execute(f"INSERT INTO {table} (currency) VALUES {text_values}")

def get_clients_for_table_Params(cursor):
    cursor.execute(f"SELECT client, exchange FROM ApiKeys")
    return cursor.fetchall()

def get_state_value(status): # ('Active', 'Passive')
    value = states_data[0] if status == 'Active' else states_data[2]
    return value

def get_trade_value(exchange: str):
    value = tradetypes_data[0]
    if exchange in ('Mexc', 'BitTeam'):
        value = tradetypes_data[1]
    return value

def get_Client_table_names():
    with sq.connect(DATABASE) as connect:
        connect.row_factory = sq.Row  # Строки записей в виде dict {}. По умолчанию - кортежи turple ()
        curs = connect.cursor()
        curs.execute(f"SELECT * FROM Clients") # name, liquid_params, shit_params
        return curs.fetchall()


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

# Создание Подчиненных Таблиц - с Параметрами для каждого клиента
    clients_table_names = get_Client_table_names()
    for row in clients_table_names:
        create_table_Params(row['liquid_params'])
        create_table_Params(row['shit_params'])

    fill_all_tables_Params()

    # drop_tables(('Clients', 'ApiKeys'))
    # drop_tables()

    # delete_rows('States', ('Pause', 'Run', 'Stop'))
    # for table in simple_tables:
    #     delete_rows(table)

    # df = import_records()
    # print(df)

