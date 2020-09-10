import apsw
import time

COMMIT_INTERVAL = 1

class Popularity:
    def __init__(self):
        self.conn = apsw.Connection("popularity.db")
        self.db = self.conn.cursor()
        self.setup()
        self.hits_since_commit = 0

        self.conn2 = apsw.Connection("/home/brewer/.local/share/lbry/lbrynet/lbrynet.sqlite",
                                     flags=apsw.SQLITE_OPEN_READONLY)
        self.db2 = self.conn2.cursor()

    def __del__(self):
        if self.hits_since_commit > 0:
            try:
                self.execute("COMMIT;")
            except:
                pass
        self.conn.close()
        self.conn2.close()

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
            CREATE TABLE IF NOT EXISTS streams
            (stream_hash   CHAR(96) NOT NULL PRIMARY KEY,
             blobs_sent    INTEGER NOT NULL,
             last_hit_time REAL NOT NULL)
            WITHOUT ROWID;""")

    def blob_to_stream(self, blob_hash):
        """
        Input:  blob hash
        Output: stream hash
        """
        result = self.db2.execute("SELECT stream_hash FROM stream_blob\
                                   WHERE blob_hash = ?;",
                                  (blob_hash, )).fetchone()[0]
        return result

    def register_hit(self, blob_hash):
        if self.hits_since_commit == 0:
            self.execute("BEGIN;")

        stream_hash = self.blob_to_stream(blob_hash)

        self.execute("""
            INSERT INTO streams VALUES (?, ?, ?)
            ON CONFLICT (stream_hash)
            DO UPDATE
            SET blobs_sent = blobs_sent + 1, last_hit_time = excluded.last_hit_time;
            """, (stream_hash, 1, time.time()))

        self.hits_since_commit += 1
        if self.hits_since_commit >= COMMIT_INTERVAL:
            self.execute("COMMIT;")
            self.hits_since_commit = 0

popularity_db = Popularity()

