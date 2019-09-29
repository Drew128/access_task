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
        self.cursor.execute(f"UPDATE subj_link SET parent = {parent_subj_id} WHERE child = {child_subj_id}")
        self.connection.commit()

    def add_access(self, subj_id: int, obj_id: int):
        subj_hrchy = pd.read_sql_query(f"SELECT * FROM subj_link", self.connection)
        child_n_parents = [subj_id]+self.find_all_parents(self, hrchy=subj_hrchy, child=subj_id)
        for subj in child_n_parents:
            self.cursor.execute("INSERT INTO access(subj_id, obj_id, owner_id) VALUES(?, ?, ?)", (subj, obj_id, subj_id))
        self.connection.commit()

    def del_access(self, subj_id: int, obj_id: int):
        self.cursor.execute(f"DELETE FROM access WHERE obj_id = {obj_id} AND owner_id = {subj_id}")
        self.connection.commit()

    def check_access(self, subj_id: int, obj_id: int) -> bool:
        accesses_df = pd.read_sql_query(f"SELECT obj_id FROM access WHERE subj_id = {subj_id}", self.connection)
        obj_hrchy = pd.read_sql_query(f"SELECT * FROM obj_link", self.connection)
        child_n_parents = [obj_id]+self.find_all_parents(self, obj_hrchy, obj_id)
        accesses = set(*accesses_df.values.tolist())
        need = set(child_n_parents)
        return bool(accesses & need)

    @staticmethod                           # should rewrite it in SQL
    def find_all_parents(self, hrchy, child):
        parents = []
        while True:
            parent = hrchy.loc[hrchy["child"] == child]['parent']
            if parent.empty:
                return parents
            parents.append(int(parent))
            child = int(parent)

    def sql_to_df(self, table_name):
        return pd.read_sql_query(f"SELECT * FROM {table_name}", self.connection)

    def read_pd(self, table_name):
        df = pd.read_sql_query(f"SELECT * FROM {table_name}", self.connection)
        return df

    @staticmethod
    def draw_tree(self, branches: list, replace=None):
        if replace is None:
            for pre, node in branches:
                print(pre, node, sep='')
        else:
            for pre, node in branches:
                print(pre, *replace.loc[replace["id"] == node]['name'].values, sep='')

    @staticmethod
    def make_nods(self, df):
        nods = {1: Node(1)}
        for _, row in df.iterrows():
            nods.update({row['child']: Node(row['child'], parent=nods[row['parent']])})
        return nods

    def tree_from_sql(self, link, handbook=None):
        rows = self.sql_to_df(table_name=link)
        replace = self.sql_to_df(table_name=handbook)
        nods = self.make_nods(self, df=rows)
        branches = [[pre, node.name] for pre, fill, node in RenderTree(nods[1])]
        self.draw_tree(self, branches=branches, replace=replace)


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

# print(db.read())

db.add_access(subj_id=6, obj_id=2)
db.add_access(subj_id=3, obj_id=2)

db.tree_from_sql(link="obj_link", handbook="object")
db.tree_from_sql(link="subj_link", handbook="subject")
# print(db.check_access(subj_id=6, obj_id=5))