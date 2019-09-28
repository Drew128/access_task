import sqlite3
import pandas as pd
import os
from anytree import Node, RenderTree


class DataBase:
    def __init__(self):
        os.remove("sqlite_first_try.db")             # debug mode
        self.connection = sqlite3.connect("sqlite_first_try.db")
        self.connection.row_factory = sqlite3.Row
        self.cursor = self.connection.cursor()
        self.cursor.execute("CREATE TABLE object (id integer PRIMARY KEY, name TEXT)")
        self.cursor.execute("INSERT INTO object(name) VALUES('WORLD')")
        self.cursor.execute("CREATE TABLE obj_link (parent integer, child integer)")
        self.cursor.execute("CREATE TABLE subject (id integer PRIMARY KEY, name TEXT)")
        self.cursor.execute("INSERT INTO subject(name) VALUES('MASTER')")
        self.cursor.execute("CREATE TABLE subj_link (parent integer, child integer)")
        self.cursor.execute("CREATE TABLE access (subj_id integer, obj_id integer, owner_id integer)")
        self.connection.commit()

    def add_obj(self, obj_name: str):
        self.cursor.execute(f"INSERT INTO object(name) VALUES('{obj_name}')")
        self.cursor.execute(f"SELECT MAX(id) last_id FROM object")
        last_id = self.cursor.fetchone()['last_id']
        self.cursor.execute("INSERT INTO obj_link(parent, child) VALUES(?, ?)", (1, last_id))
        self.connection.commit()

    def add_obj_link(self, parent_obj_id: int, child_obj_id: str):
        # self.cursor.execute("INSERT INTO obj_link(parent, child) VALUES(?, ?)", (parent_obj_id, child_obj_id))
        self.cursor.execute(f"UPDATE obj_link SET parent = {parent_obj_id} WHERE child = {child_obj_id}")
        self.connection.commit()

    def add_subj(self, subj_name: str):
        self.cursor.execute(f"INSERT INTO subject(name) VALUES('{subj_name}')")
        self.cursor.execute(f"SELECT MAX(id) last_id FROM subject")
        last_id = self.cursor.fetchone()['last_id']
        self.cursor.execute("INSERT INTO subj_link(parent, child) VALUES(?, ?)", (1, last_id))
        self.connection.commit()

    def add_subj_link(self, parent_subj_id: int, child_subj_id: str):
        #self.cursor.execute("INSERT INTO subj_link(parent, child) VALUES(?, ?)", (parent_subj_id, child_subj_id))
        self.cursor.execute(f"UPDATE subj_link SET parent = {parent_subj_id} WHERE child = {child_subj_id}")
        self.connection.commit()

    def add_access(self, subj_id: int, obj_id: int):
        self.cursor.execute("INSERT INTO access(subj_id, obj_id) VALUES(?, ?)", (subj_id, obj_id))
        self.connection.commit()

    def read_pd(self, table_name):
        df = pd.read_sql_query(f"SELECT * FROM {table_name}", self.connection)
        return df

    def read(self):     # should change to id`s
        self.cursor.execute("SELECT p.name parent, c.name child "
                            "FROM obj_link o "
                            "LEFT JOIN object p ON p.id = o.parent "
                            "LEFT JOIN object c ON c.id = o.child")
        rows = self.cursor.fetchall()
        nods = {"WORLD": Node("WORLD")}
        # marc = Node("Marc", parent=udo)
        for row in rows:
            nods.update({row['child']: Node(row['child'], parent=nods[row['parent']])})
            print(row['parent'], row['child'])

        for pre, fill, node in RenderTree(nods["WORLD"]):
            print(pre, node.name, sep='')



db = DataBase()
[db.add_obj(obj_name=obj_name)
 for obj_name in ["Europe", 'Ukraine', 'Kyiv', 'Poland', 'Krakow']]


[db.add_obj_link(parent_obj_id=parent_obj_id, child_obj_id=child_obj_id)
 for parent_obj_id, child_obj_id in [[2, 3], [3, 4], [2, 5], [5, 6]]]


[db.add_subj(subj_name=subj_name)
 for subj_name in ["Mykola", "Yar", 'Vanya', 'Iolande', 'Ihor', 'Drew']]


[db.add_subj_link(parent_subj_id=parent_subj_id, child_subj_id=child_subj_id)
 for parent_subj_id, child_subj_id in [[2, 3], [3, 4], [4, 5], [3, 6], [6, 7]]]

# print(db.read_pd("obj_link"))
# print(db.read_pd("object"))
# print(db.read_pd("subject"))
# print(db.read_pd("subj_link"))

print(db.read())