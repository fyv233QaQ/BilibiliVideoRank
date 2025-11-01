import sqlite3

class dataBase:

    def __init__(self, name):
        self.name = name
        self.conn = sqlite3.connect(name)
        self.cur = self.conn.cursor()

    async def createTable(self, tableName):
        try:
            self.cur.execute(f"""
            CREATE TABLE {tableName} (
                bvid            TEXT     NOT NULL,
                score           FLOAT    NOT NULL,
                PRIMARY KEY (bvid)
            )""")
        except Exception as e:
            print(f"创建表时出错: {e}")
        self.conn.commit()

    async def updateTable(self, tableName, content):
        try:
            # 假设content是一个元组列表，每个元组是(bvid, score)
            for bvid, score in content:
                self.cur.execute(f"""INSERT OR IGNORE INTO {tableName} (bvid, score) VALUES (?, ?)""", (bvid, score))
                self.cur.execute(f"""UPDATE {tableName} SET score = ? WHERE bvid = ?""", (score, bvid))
        except Exception as e:
            print(f"更新表时出错: {e}")
        self.conn.commit()
