import apsw
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
            (blob_hash         TEXT NOT NULL PRIMARY KEY,
             count_seeds       INTEGER NOT NULL DEFAULT 0,
             last_seeded_epoch INTEGER)
            WITHOUT ROWID;""")

        self.db.execute("COMMIT;")


    def save(self):

        self.db.execute("BEGIN;")
        for item in self.seeded:

            # Update seed counts and last seed time for the blob
            self.db.execute("""
                INSERT INTO blob VALUES (?, 1, ?)
                ON CONFLICT (blob_hash)
                DO UPDATE SET count_seeds = count_seeds + 1,
                              last_seeded_epoch = excluded.last_seeded_epoch;""",
                item)
        self.db.execute("COMMIT;")


    def log_seed(self, blob_hash):
        """
        Log that a particular blob was seeded.
        """
        now = time.time()
        self.seeded.append((blob_hash, now))

        if len(self.seeded) >= 100 or \
            (len(self.seeded) > 0 and now - self.seeded[0][1] >= 3600):
            self.save()


