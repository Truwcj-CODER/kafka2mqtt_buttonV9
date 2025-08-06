import sqlite3
import os

class SQLiteDeviceLineData:
    def __init__(self, db_path="device_data.db"):
        self.db_path = db_path
        self.conn = sqlite3.connect(self.db_path)
        self.cursor = self.conn.cursor()
        self._init_db()

    def _init_db(self):
        """Khởi tạo DB và bảng nếu chưa có"""
        try:
            self.cursor.execute("""
                CREATE TABLE IF NOT EXISTS device_line_data (
                    machine_code TEXT PRIMARY KEY,
                    line1 TEXT,
                    line2 TEXT
                )
            """)
            self.conn.commit()
            print("✅ Đã khởi tạo bảng SQLite.")
        except Exception as e:
            print("🔴 Lỗi khi khởi tạo DB:", e)

    def machine_code_exists(self, machine_code):
        """Kiểm tra machine_code có tồn tại trong DB hay chưa"""
        try:
            self.cursor.execute("SELECT 1 FROM device_line_data WHERE machine_code = ?", (machine_code.lower(),))
            return self.cursor.fetchone() is not None
        except Exception as e:
            print("🔴 Lỗi khi kiểm tra machine_code:", e)
            return False

    # def upsert_machine_data(self, machine_code, line1=None, line2=None):
    #     """Nếu tồn tại thì update, không thì insert"""
    #     try:
    #         machine_code = machine_code.lower()
    #         line1 = line1 or "test1"
    #         line2 = line2 or "test2"

    #         self.cursor.execute("""
    #             INSERT INTO device_line_data (machine_code, line1, line2)
    #             VALUES (?, ?, ?)
    #             ON CONFLICT(machine_code) DO UPDATE SET
    #                 line1 = excluded.line1,
    #                 line2 = excluded.line2
    #         """, (machine_code, line1, line2))
    #         self.conn.commit()
    #         print(f"💾 Dữ liệu đã được ghi: {machine_code} | line1: {line1}, line2: {line2}")
    #     except Exception as e:
    #         print("🔴 Lỗi khi ghi hoặc cập nhật dữ liệu:", e)

    def upsert_machine_data(self, machine_code, line2):
        """Nếu tồn tại thì update line2, không thì insert"""
        try:
            machine_code = machine_code.lower()
            self.cursor.execute("""
                INSERT INTO device_line_data (machine_code, line2)
                VALUES (?, ?)
                ON CONFLICT(machine_code) DO UPDATE SET
                    line2 = excluded.line2
            """, (machine_code, line2))
            self.conn.commit()
            print(f"💾 Dữ liệu đã được ghi: {machine_code} | line2: {line2}")
        except Exception as e:
            print("🔴 Lỗi khi ghi hoặc cập nhật dữ liệu:", e)

    def get_all(self):
        """Lấy toàn bộ dữ liệu"""
        try:
            self.cursor.execute("SELECT machine_code, line1, line2 FROM device_line_data")
            return self.cursor.fetchall()
        except Exception as e:
            print("🔴 Lỗi khi lấy dữ liệu:", e)
            return []

    def get_device_data(self, machine_code):
        """Lấy dữ liệu cho một machine_code cụ thể"""
        try:
            machine_code = machine_code.lower()
            self.cursor.execute("SELECT line1, line2 FROM device_line_data WHERE machine_code = ?", (machine_code,))
            row = self.cursor.fetchone()
            if row:
                return {"line1": row[0], "line2": row[1]}
            return None
        except Exception as e:
            print("🔴 Lỗi get_device_data:", e)
            return None

    def close(self):
        """Đóng kết nối DB"""
        try:
            self.conn.close()
            print("🔒 Đã đóng kết nối tới SQLite.")
        except Exception as e:
            print("🔴 Lỗi khi đóng kết nối:", e)
