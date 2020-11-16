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
        # Transaction management
        self.in_transaction = False
        self.last_commit = 0.0

        self.conn = apsw.Connection(DB_FILENAME)
        self.conn.setbusytimeout(5000)

        self.db = self.conn.cursor()
        self.setup_database()


    def __del__(self):
        self.conn.close()

    def setup_database(self):
        self.db.execute("PRAGMA SYNCHRONOUS = 0;")
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
            (start_time      INTEGER NOT NULL PRIMARY KEY,
             blobs_up        INTEGER NOT NULL,
             blobs_down      INTEGER NOT NULL,
             blobs_announced INTEGER NOT NULL)
            WITHOUT ROWID;""")

        self.db.execute("COMMIT;")


    def log_seed(self, blob_hash):
        """
        Log that a particular blob was seeded.
        """
        now = time.time()

        self.db.execute("BEGIN;")

        # Update seed counts and last seed time for the blob
        self.db.execute("""
            INSERT INTO blob VALUES (?, 1, ?)
            ON CONFLICT (blob_hash)
            DO UPDATE SET
                seed_count = seed_count + 1,
                last_seed_time = excluded.last_seed_time;""",
            (blob_hash, now))

        # Update hour table
        hour = datetime.datetime.fromtimestamp(now)
        hour = hour.replace(minute=0, second=0, microsecond=0)
        self.db.execute("""
            INSERT INTO hour VALUES (?, 1, 0, 0)
            ON CONFLICT (start_time)
            DO UPDATE SET blobs_up = blobs_up + 1;
            """, (int(hour.timestamp()), ))

        self.db.execute("COMMIT;")


    def log_download(self):
        """
        Log that a blob was downloaded.
        """
        now = time.time()
        self.db.execute("BEGIN;")

        # Update hour table
        hour = datetime.datetime.fromtimestamp(now)
        hour = hour.replace(minute=0, second=0, microsecond=0)
        self.db.execute("""
            INSERT INTO hour VALUES (?, 0, 1, 0)
            ON CONFLICT (start_time)
            DO UPDATE SET blobs_down = blobs_down + 1;
            """, (int(hour.timestamp()), ))

        self.db.execute("COMMIT;")


    def log_announcement(self):
        """
        Log that a blob was announced
        """
        now = time.time()
        self.db.execute("BEGIN;")

        # Update hour table
        hour = datetime.datetime.fromtimestamp(now)
        hour = hour.replace(minute=0, second=0, microsecond=0)
        self.db.execute("""
            INSERT INTO hour VALUES (?, 0, 0, 1)
            ON CONFLICT (start_time)
            DO UPDATE SET blobs_announced = blobs_announced + 1;
            """, (int(hour.timestamp()), ))

        self.db.execute("COMMIT;")


