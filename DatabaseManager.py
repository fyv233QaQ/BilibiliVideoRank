import asyncio
import sqlite3
import time
from datetime import datetime, timedelta
from typing import List, Dict, Any


class DatabaseManager:
    def __init__(self, name="database.db"):
        self.name = name
        self.init_update_log()

    def get_connection(self):
        return sqlite3.connect(self.name)

    def init_update_log(self):
        """初始化更新日志表"""
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS update_log (
                table_name TEXT PRIMARY KEY,
                last_updated TIMESTAMP,
                update_count INTEGER DEFAULT 0
            )
        ''')
        conn.commit()
        conn.close()

    def get_all_tables(self) -> List[str]:
        """获取所有表名（排除系统表）"""
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name != 'update_log';")
        tables = [table[0] for table in cursor.fetchall()]
        conn.close()
        return tables

    def get_table_structure(self, table_name: str) -> List[Dict]:
        """获取表结构"""
        conn = self.get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute(f"PRAGMA table_info({table_name})")
            columns = cursor.fetchall()
            return [{"name": col[1], "type": col[2]} for col in columns]
        except sqlite3.Error as e:
            print(f"获取表结构错误: {e}")
            return []
        finally:
            conn.close()

    def get_table_data_safe(self, table_name: str, limit: int = 100) -> List[Dict[str, Any]]:
        """安全地获取表数据"""
        conn = self.get_connection()
        cursor = conn.cursor()

        try:
            cursor.execute(f"PRAGMA table_info({table_name})")
            columns = [col[1] for col in cursor.fetchall()]

            cursor.execute(f"SELECT * FROM {table_name} LIMIT ?", (limit,))
            rows = cursor.fetchall()

            result = []
            for row in rows:
                row_dict = {}
                for i, column_name in enumerate(columns):
                    row_dict[column_name] = row[i]
                result.append(row_dict)

            return result
        except sqlite3.Error as e:
            print(f"数据库错误: {e}")
            return []
        finally:
            conn.close()

    def get_table_data_paginated(self, table_name: str, page: int = 1, per_page: int = 20,
                                 sort_by: str = "score", order: str = "DESC") -> Dict:
        """获取分页数据"""
        conn = self.get_connection()
        cursor = conn.cursor()

        try:
            cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
            total_count = cursor.fetchone()[0]

            offset = (page - 1) * per_page

            cursor.execute(f"PRAGMA table_info({table_name})")
            columns = [col[1] for col in cursor.fetchall()]

            query = f"SELECT * FROM {table_name} ORDER BY {sort_by} {order} LIMIT ? OFFSET ?"
            cursor.execute(query, (per_page, offset))
            rows = cursor.fetchall()

            data = []
            for row in rows:
                row_dict = {}
                for i, column_name in enumerate(columns):
                    row_dict[column_name] = row[i]
                data.append(row_dict)

            total_pages = (total_count + per_page - 1) // per_page

            return {
                "data": data,
                "total_count": total_count,
                "total_pages": total_pages,
                "page": page,
                "per_page": per_page
            }
        except sqlite3.Error as e:
            print(f"数据库错误: {e}")
            return {"data": [], "total_count": 0, "total_pages": 0, "page": page, "per_page": per_page}
        finally:
            conn.close()

    def search_videos(self, table_name: str, search_term: str = "", sort_by: str = "score", order: str = "DESC") -> \
    List[Dict[str, Any]]:
        """搜索视频数据"""
        conn = self.get_connection()
        cursor = conn.cursor()

        try:
            cursor.execute(f"PRAGMA table_info({table_name})")
            columns = [col[1] for col in cursor.fetchall()]

            if search_term:
                conditions = []
                params = []
                for col in columns:
                    conditions.append(f"{col} LIKE ?")
                    params.append(f'%{search_term}%')

                where_clause = " OR ".join(conditions)
                query = f"SELECT * FROM {table_name} WHERE {where_clause} ORDER BY {sort_by} {order}"
                cursor.execute(query, params)
            else:
                cursor.execute(f"SELECT * FROM {table_name} ORDER BY {sort_by} {order}")

            rows = cursor.fetchall()

            result = []
            for row in rows:
                row_dict = {}
                for i, column_name in enumerate(columns):
                    row_dict[column_name] = row[i]
                result.append(row_dict)

            return result
        except sqlite3.Error as e:
            print(f"搜索错误: {e}")
            return []
        finally:
            conn.close()

    def get_video_count(self, table_name: str) -> int:
        """获取表中视频数量"""
        conn = self.get_connection()
        cursor = conn.cursor()

        try:
            cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
            count = cursor.fetchone()[0]
            return count
        except sqlite3.Error:
            return 0
        finally:
            conn.close()

    def can_update(self, table_name: str) -> bool:
        """检查是否可以更新（3小时限制）"""
        conn = self.get_connection()
        cursor = conn.cursor()

        cursor.execute("SELECT last_updated FROM update_log WHERE table_name = ?", (table_name,))
        result = cursor.fetchone()
        conn.close()

        if not result:
            return True

        last_updated = datetime.fromisoformat(result[0])
        now = datetime.now()
        time_diff = now - last_updated

        return time_diff.total_seconds() >= 3 * 3600  # 3小时

    def get_last_update_time(self, table_name: str) -> str:
        """获取最后更新时间"""
        conn = self.get_connection()
        cursor = conn.cursor()

        cursor.execute("SELECT last_updated, update_count FROM update_log WHERE table_name = ?", (table_name,))
        result = cursor.fetchone()
        conn.close()

        if result:
            last_updated = datetime.fromisoformat(result[0])
            update_count = result[1]
            next_update = last_updated + timedelta(hours=3)
            now = datetime.now()

            if now < next_update:
                remaining = next_update - now
                hours = int(remaining.total_seconds() // 3600)
                minutes = int((remaining.total_seconds() % 3600) // 60)
                return f"最后更新: {last_updated.strftime('%Y-%m-%d %H:%M:%S')} (还剩 {hours}小时{minutes}分钟可更新, 已更新{update_count}次)"
            else:
                return f"最后更新: {last_updated.strftime('%Y-%m-%d %H:%M:%S')} (可以更新, 已更新{update_count}次)"
        else:
            return "从未更新"

    def log_update(self, table_name: str):
        """记录更新日志"""
        conn = self.get_connection()
        cursor = conn.cursor()

        now = datetime.now().isoformat()
        cursor.execute('''
            INSERT OR REPLACE INTO update_log (table_name, last_updated, update_count)
            VALUES (?, ?, COALESCE((SELECT update_count FROM update_log WHERE table_name = ?), 0) + 1)
        ''', (table_name, now, table_name))

        conn.commit()
        conn.close()