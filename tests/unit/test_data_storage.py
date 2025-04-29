# import sqlite3
# from analyzer.storage import insert_notice, get_notice_by_id

# def test_insert_and_get_notice():
#     conn = sqlite3.connect(":memory:")
#     conn.execute("CREATE TABLE notices (id TEXT PRIMARY KEY, title TEXT)")
    
#     insert_notice(conn, {"id": "TED001", "title": "Test"})
#     notice = get_notice_by_id(conn, "TED001")

#     assert notice["title"] == "Test"
