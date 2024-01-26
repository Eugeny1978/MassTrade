import sqlite3 as sq
from date_base.path_to_base import DATABASE


# cursor_bd.execute("""CREATE TABLE IF NOT EXISTS users (
#     user_id INTEGER PRIMARY KEY AUTOINCREMENT,
#     name TEXT NOT NULL,
#     sex INTEGER NOT NULL DEFAULT 1,
#     old INTEGER,
#     score INTEGER)""")

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
        params_table TEXT UNIQUE
        )""")

def create_table_ApiKeys():
    with sq.connect(DATABASE) as connect:
        curs = connect.cursor()
        curs.execute(f"""CREATE TABLE IF NOT EXISTS ApiKeys (
        client TEXT,
        exchange TEXT,
        apiKey TEXT,
        secret TEXT,
        password TEXT,
        state TEXT,
        active_date TEXT,
        FOREIGN KEY(client) REFERENCES Clients(name) ON DELETE SET NULL,
        FOREIGN KEY(exchange) REFERENCES Exchanges(name) ON DELETE SET NULL,
        FOREIGN KEY(state) REFERENCES States(name) ON DELETE SET NULL
        )""")

def fill_simple_table(table_name: str, data: tuple or list):
    with sq.connect(DATABASE) as connect:
        curs = connect.cursor()
        for value in data:
            curs.execute(f"""SELECT name FROM {table_name} WHERE name IS '{value}'""")
            responce = curs.fetchone()
            if not responce:
                # INSERT INTO users (name, age) VALUES ('Tom', 37)
                curs.execute(f"""INSERT INTO {table_name} (name) VALUES ('{value}')""")

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





if __name__ == '__main__':

    simple_tables = ('Currencies', 'States', 'TradeTypes')
    currencies_data = ('ATOM', 'BTC', 'ETH', 'MATIC', 'XRP')
    states_data = ('Run', 'Pause', 'Stop')
    tradetypes_data = ('Liquid_coins', 'Shit_coins')
    simple_datas = {simple_tables[0]: currencies_data,
                    simple_tables[1]: states_data,
                    simple_tables[2]: tradetypes_data}

    for table_name in simple_tables:
        create_simple_table(table_name)

    create_table_Bots()
    create_table_Exchanges()
    create_table_Clients()
    create_table_ApiKeys()

    for table, data in simple_datas.items():
        fill_simple_table(table, data)

    # drop_tables(('Clients', 'ApiKeys'))
    # drop_tables()

    # delete_rows('States', ('Pause', 'Run', 'Stop'))
    # for table in simple_tables:
    #     delete_rows(table)

