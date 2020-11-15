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
        self.transaction_active = False
        self.setup_database()

    def begin(self):
        """
        Open transaction.
        """
        if not self.transaction_active:
            self.db.execute("BEGIN;")
            self.transaction_active = True

    def commit(self):
        """
        Commit transaction
        """
        if self.transaction_active:
            self.db.execute("COMMIT;")
            self.transaction_active = False

    def __del__(self):
        self.conn.close()

    def setup_database(self):
        self.db.execute("PRAGMA JOURNAL_MODE = WAL;")
        self.db.execute("BEGIN;")
        self.db.execute("""
            CREATE TABLE IF NOT EXISTS blob
            (blob_hash        TEXT NOT NULL PRIMARY KEY,
             count_seeds      INTEGER NOT NULL DEFAULT 0,
             last_seeded_time REAL)
            WITHOUT ROWID;""")
        self.db.execute("COMMIT;")


    def log_seed(self, blob_hash):
        """
        Log that a particular blob was seeded.
        """
        self.begin()
        self.db.execute("""
            INSERT INTO blob VALUES (?, 1, ?)
            ON CONFLICT (blob_hash)
            DO UPDATE SET count_seeds = count_seeds + 1,
                          last_seeded_time = excluded.last_seeded_time;""",
            (blob_hash, time.time()))
        self.commit()


