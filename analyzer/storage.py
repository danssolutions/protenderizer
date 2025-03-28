import sqlite3

def get_connection():
    pass

def insert_notice_if_new(notice):
    pass

def insert_notice(conn, notice):
    pass

def get_notice_by_id(conn, notice_id):
    pass

# Dummy storage backends for now
class SQLStorage:
    pass

class NoSQLStorage:
    pass
