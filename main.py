import sqlite3
import pandas as pd
import os


class DataBase:
    def __init__(self):
        os.remove("sqlite_first_try.db")             # debug mode
        self.connection = sqlite3.connect("sqlite_first_try.db")
        self.connection.row_factory = sqlite3.Row
        self.cursor = self.connection.cursor()
        self.cursor.execute("CREATE TABLE object (id integer PRIMARY KEY, name TEXT)")
        self.cursor.execute("CREATE TABLE obj_link (parent integer, child integer)")
        self.cursor.execute("CREATE TABLE subject (id integer PRIMARY KEY, name TEXT)")
        self.cursor.execute("CREATE TABLE subj_link (parent integer, child integer)")
        self.cursor.execute("CREATE TABLE access (subj_id integer, obj_id integer)")
        self.connection.commit()

    def add_obj(self, obj_id: int, obj_name: str):
        self.cursor.execute("INSERT INTO object(id, name) VALUES(?, ?)", (obj_id, obj_name))
        self.connection.commit()

    def add_obj_link(self, parent_obj_id: int, child_obj_id: str):
        self.cursor.execute("INSERT INTO obj_link(parent, child) VALUES(?, ?)", (parent_obj_id, child_obj_id))
        self.connection.commit()

    def add_subj(self, subj_id: int, subj_name: str):
        self.cursor.execute("INSERT INTO subject(id, name) VALUES(?, ?)", (subj_id, subj_name))
        self.connection.commit()

    def add_subj_link(self, parent_subj_id: int, child_subj_id: str):
        self.cursor.execute("INSERT INTO subj_link(parent, child) VALUES(?, ?)", (parent_subj_id, child_subj_id))
        self.connection.commit()

    def add_access(self, subj_id: int, obj_id: int):
        self.cursor.execute("INSERT INTO access(subj_id, obj_id) VALUES(?, ?)", (subj_id, obj_id))
        self.connection.commit()

    def read_pd(self, table_name):
        df = pd.read_sql_query(f"SELECT * FROM {table_name}", self.connection)
        return df


db = DataBase()
[db.add_obj(obj_id=obj_id, obj_name=obj_name)
 for obj_id, obj_name in [[1, "Europe"], [5, 'Ukraine'], [7, 'Kyiv'], [12, 'Krakow'], [16, 'Telaviv']]]
print(db.read_pd("object"))

[db.add_obj_link(parent_obj_id=parent_obj_id, child_obj_id=child_obj_id)
 for parent_obj_id, child_obj_id in [[1, 5], [5, 7]]]
print(db.read_pd("obj_link"))

[db.add_subj(subj_id=subj_id, subj_name=subj_name)
 for subj_id, subj_name in [[0, "Yar"], [7, 'Vanya'], [777, 'Iolanta']]]
print(db.read_pd("subject"))

[db.add_subj_link(parent_subj_id=parent_subj_id, child_subj_id=child_subj_id)
 for parent_subj_id, child_subj_id in [[0, 7], [7, 777]]]

print(db.read_pd("subj_link"))
