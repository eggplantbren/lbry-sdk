import apsw
import datetime
import time

# TODO: Use conf data_dir
DB_FILENAME = "/home/brewer/.local/share/lbry/lbrynet/data_stats.db"
DATA_STATS_ENABLED = True

class DataStats:
    """
    Manages a connection to the data_stats database.
    """

    def __init__(self):
        self.conn = apsw.Connection(DB_FILENAME)
        self.db = self.conn.cursor()
        self.setup_database()

        # List of recently seeded blobs
        self.seeded = []

    def __del__(self):
        self.save()
        self.conn.close()

    def setup_database(self):
        self.db.execute("PRAGMA JOURNAL_MODE = WAL;")
        self.db.execute("BEGIN;")

        self.db.execute("""
            CREATE TABLE IF NOT EXISTS blob
            (blob_hash      TEXT NOT NULL PRIMARY KEY,
             seed_count     INTEGER NOT NULL,
             last_seed_time REAL NOT NULL)
            WITHOUT ROWID;""")


        self.db.execute("""
            CREATE TABLE IF NOT EXISTS hour
            (start_time INTEGER NOT NULL PRIMARY KEY,
             blobs_up   INTEGER NOT NULL,
             blobs_down INTEGER NOT NULL)
            WITHOUT ROWID;""")

        self.db.execute("COMMIT;")


    def save(self):

        self.db.execute("BEGIN;")

        for item in self.seeded:

            blob_hash, epoch = item

            # Update seed counts and last seed time for the blob
            self.db.execute("""
                INSERT INTO blob VALUES (?, 1, ?)
                ON CONFLICT (blob_hash)
                DO UPDATE SET
                    seed_count = seed_count + 1,
                    last_seed_time = excluded.last_seed_time;""",
                (blob_hash, epoch))

            # Update hour table
            hour = datetime.datetime.fromtimestamp(epoch)
            hour = hour.replace(minute=0, second=0, microsecond=0)
            self.db.execute("""
                INSERT INTO hour VALUES (?, 1, 0)
                ON CONFLICT (start_time)
                DO UPDATE SET blobs_up = blobs_up + 1;
                """, (int(hour.timestamp()), ))

        self.db.execute("COMMIT;")

        # Clear in-memory data
        self.seeded = []


    def log_seed(self, blob_hash):
        """
        Log that a particular blob was seeded.
        """
        now = time.time()
        self.seeded.append((blob_hash, now))

        if len(self.seeded) >= 100 or \
            (len(self.seeded) > 0 and now - self.seeded[0][1] >= 3600):
            self.save()


