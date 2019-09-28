import sqlite3
import pandas as pd


class DataBase:
    def __init__(self):
        self.connection = sqlite3.connect("sqlite_first_try.db")
        self.connection.row_factory = sqlite3.Row
        self.cursor = self.connection.cursor()
        # self.cursor.execute("CREATE TABLE object (id integer PRIMARY KEY, name TEXT)")
        # self.cursor.execute("CREATE TABLE link_obj (parent integer, child integer)")
        # self.cursor.execute("CREATE TABLE subject (id integer PRIMARY KEY, name TEXT)")
        # self.cursor.execute("CREATE TABLE link_subj (parent integer, child integer)")
        # self.cursor.execute("CREATE TABLE access (subj_id integer, obj_id integer)")
        # self.connection.commit()

    def add_obj(self, obj_id: int, obj_name: str):
        self.cursor.execute("INSERT INTO object(id, name) VALUES(?, ?)", (obj_id, obj_name))
        print(self.connection.commit())

    def read(self, table_name):
        self.cursor.execute(f"SELECT * FROM {table_name}")
        rows = self.cursor.fetchall()
        keys = rows[0].keys()
        print(keys)
        for row in rows:
            print([row[key] for key in row.keys()])

    def read_pd(self, table_name):
        df = pd.read_sql_query(f"SELECT * FROM {table_name}", self.connection)
        return df


db = DataBase()
#db.add_obj(obj_id=1, obj_name="Europe")
#db.add_obj(obj_id=5, obj_name="Ukraine")
print(db.read_pd("object"))

