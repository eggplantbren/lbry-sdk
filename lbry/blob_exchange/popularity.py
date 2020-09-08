import apsw
import time

COMMIT_INTERVAL = 10

class Popularity:
    def __init__(self):
        self.conn = apsw.Connection("blob_popularity.db")
        self.db = self.conn.cursor()
        self.setup()
        self.hits_since_commit = 0

    def execute(self, *args, **kwargs):
        return self.db.execute(*args, **kwargs)

    def setup(self):
        self.pragmas()
        self.execute("BEGIN;")
        self.create_tables()
        self.execute("COMMIT;")

    def pragmas(self):
        self.execute("PRAGMA JOURNAL_MODE = WAL;")
        self.execute("PRAGMA SYNCHRONOUS = 0;")

    def create_tables(self):
        self.execute("""
            CREATE TABLE IF NOT EXISTS blobs
            (blob_hash     CHAR(96) NOT NULL PRIMARY KEY,
             hits          INTEGER NOT NULL,
             last_hit_time REAL NOT NULL)
            WITHOUT ROWID;""")

    def register_hit(self, blob_hash):
        if self.hits_since_commit == 0:
            self.execute("BEGIN;")

        self.execute("""
            INSERT INTO blobs VALUES (?, ?, ?)
            ON CONFLICT (blob_hash)
            DO UPDATE
            SET hits = hits + 1, last_hit_time = excluded.last_hit_time;
            """, (blob_hash, 1, time.time()))

        self.hits_since_commit += 1
        if self.hits_since_commit >= COMMIT_INTERVAL:
            self.execute("COMMIT;")
            self.hits_since_commit = 0

popularity_db = Popularity()

