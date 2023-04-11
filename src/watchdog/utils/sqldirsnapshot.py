import os
import sqlite3
from stat import S_ISDIR

from watchdog.utils.dirsnapshot import DirectorySnapshot


class SqliteDirectorySnapshot(DirectorySnapshot):
    def __init__(self, path, recursive=True, stat=os.stat, listdir=os.scandir):
        self.conn = sqlite3.connect(":memory:")
        self.create_tables()

        super().__init__(path, recursive, stat, listdir)

    def create_tables(self):
        c = self.conn.cursor()
        c.execute(
            """
            CREATE TABLE stat_info (
                path TEXT PRIMARY KEY,
                st_ino INTEGER,
                st_dev INTEGER,
                st_mode INTEGER,
                st_mtime REAL,
                st_size INTEGER
            )
            """
        )
        self.conn.commit()

    def walk(self, root):
        for p, st in super().walk(root):
            self.insert_stat_info(p, st)
            yield p, st

    def insert_stat_info(self, path, st):
        c = self.conn.cursor()
        c.execute(
            """
            INSERT INTO stat_info (path, st_ino, st_dev, st_mode, st_mtime, st_size)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (path, st.st_ino, st.st_dev, st.st_mode, st.st_mtime, st.st_size),
        )
        self.conn.commit()

    @property
    def paths(self):
        c = self.conn.cursor()
        c.execute("SELECT path FROM stat_info")
        return {row[0] for row in c.fetchall()}

    def path(self, id):
        c = self.conn.cursor()
        c.execute("SELECT path FROM stat_info WHERE st_ino=? AND st_dev=?", id)
        row = c.fetchone()
        return row[0] if row else None

    def inode(self, path):
        c = self.conn.cursor()
        c.execute("SELECT st_ino, st_dev FROM stat_info WHERE path=?", (path,))
        row = c.fetchone()
        return (row[0], row[1]) if row else None

    def isdir(self, path):
        c = self.conn.cursor()
        c.execute("SELECT st_mode FROM stat_info WHERE path=?", (path,))
        row = c.fetchone()
        return S_ISDIR(row[0]) if row else None

    def mtime(self, path):
        c = self.conn.cursor()
        c.execute("SELECT st_mtime FROM stat_info WHERE path=?", (path,))
        row = c.fetchone()
        return row[0] if row else None

    def size(self, path):
        c = self.conn.cursor()
        c.execute("SELECT st_size FROM stat_info WHERE path=?", (path,))
        row = c.fetchone()
        return row[0] if row else None

    def stat_info(self, path):
        c = self.conn.cursor()
        c.execute(
            "SELECT st_ino, st_dev, st_mode, st_mtime, st_size FROM stat_info WHERE path=?",
            (path,),
        )
        row = c.fetchone()
        if row:
            st = os.stat_result((0, row[0], row[1], row[2], 0, 0, row[4], row[3], 0, 0))
            return st
        else:
            return None

    def __del__(self):
        self.conn.close()
