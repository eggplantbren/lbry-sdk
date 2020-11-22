import apsw
import datetime
#from lbry.conf import Config
import math
import time

# TODO: Use conf data_dir
DB_FILENAME = "/home/brewer/.local/share/lbry/lbrynet/data_stats.db"
DATA_STATS_ENABLED = True

class DataStats:
    """
    Manages connections to the data_stats database.
    TODO: Consult with Jack on how to use asyncio instead of having a different
          connection for each function.
    """

    def __init__(self):
        self.connections = {}
        for key in ["setup", "log_seed", "log_download", "log_announcement"]:
            self.connections[key] = apsw.Connection(DB_FILENAME)
        self.setup_database()


    def __del__(self):
        for key in self.connections:
            self.connections[key].close()

    def setup_database(self):
        db = self.connections["setup"].cursor()
        db.execute("PRAGMA SYNCHRONOUS = 0;")
        db.execute("PRAGMA JOURNAL_MODE = WAL;")
        db.execute("BEGIN;")

        db.execute("""
            CREATE TABLE IF NOT EXISTS blob
            (blob_hash      TEXT NOT NULL PRIMARY KEY,
             seed_count     INTEGER NOT NULL,
             last_seed_time REAL NOT NULL,
             popularity     REAL NOT NULL)
            WITHOUT ROWID;""")

        db.execute("""
            CREATE TABLE IF NOT EXISTS hour
            (start_time      INTEGER NOT NULL PRIMARY KEY,
             blobs_up        INTEGER NOT NULL,
             blobs_down      INTEGER NOT NULL,
             blobs_announced INTEGER NOT NULL)
            WITHOUT ROWID;""")

        db.execute("COMMIT;")



    def log_seed(self, blob_hash):
        """
        Log that a particular blob was seeded.
        """
        now = time.time()

        db = self.connections["log_seed"].cursor()
        db.execute("BEGIN;")

        # Insert if necessary
        db.execute("""
            INSERT INTO blob VALUES (?, 1, ?, 0.0)
            ON CONFLICT (blob_hash) DO NOTHING;""", (blob_hash, now))

        # Get last seed time and popularity
        last, pop = db.execute("SELECT last_seed_time FROM blob\
                                     WHERE blob_hash = ?;",
                                    (blob_hash, )).fetchone()

        # Updated popularity
        pop = pop*math.exp(-(now - last)/(7*86400)) + 1.0

        db.execute("""
            UPDATE blob SET
                seed_count     = seed_count + 1,
                last_seed_time = ?,
                popularity     = ?
            WHERE blob_hash = ?;""", (now, pop, blob_hash))

        # Update hour table
        hour = datetime.datetime.fromtimestamp(now)
        hour = hour.replace(minute=0, second=0, microsecond=0)
        db.execute("""
            INSERT INTO hour VALUES (?, 1, 0, 0)
            ON CONFLICT (start_time)
            DO UPDATE SET blobs_up = blobs_up + 1;
            """, (int(hour.timestamp()), ))

        db.execute("COMMIT;")


    def log_download(self):
        """
        Log that a blob was downloaded.
        """
        now = time.time()
        db = self.connections["log_download"].cursor()
        db.execute("BEGIN;")

        # Update hour table
        hour = datetime.datetime.fromtimestamp(now)
        hour = hour.replace(minute=0, second=0, microsecond=0)
        db.execute("""
            INSERT INTO hour VALUES (?, 0, 1, 0)
            ON CONFLICT (start_time)
            DO UPDATE SET blobs_down = blobs_down + 1;
            """, (int(hour.timestamp()), ))

        db.execute("COMMIT;")



    def log_announcement(self):
        """
        Log that a blob was announced
        """
        now = time.time()
        db = self.connections["log_announcement"].cursor()
        db.execute("BEGIN;")

        # Update hour table
        hour = datetime.datetime.fromtimestamp(now)
        hour = hour.replace(minute=0, second=0, microsecond=0)
        db.execute("""
            INSERT INTO hour VALUES (?, 0, 0, 1)
            ON CONFLICT (start_time)
            DO UPDATE SET blobs_announced = blobs_announced + 1;
            """, (int(hour.timestamp()), ))

        db.execute("COMMIT;")

    def find_unpopular_streams(self):
        conn = apsw.Connection(DB_FILENAME, flags=apsw.SQLITE_OPEN_READONLY)
        db = conn.cursor()
        db.execute("BEGIN;")
        db.execute("ATTACH DATABASE '/home/brewer/.local/share/lbry/lbrynet/lbrynet.sqlite' AS lbrynet;")

        # This query finds the 10 least popular streams for seeding
        # TODO: Use the timestamp that the stream was added, to avoid penalising
        # things you just downloaded
        for row in db.execute("""
            SELECT sb.stream_hash, MAX(mb.seed_count) max_seed_count,
                MAX(mb.last_seed_time) max_seed_time FROM
                lbrynet.blob lb
                INNER JOIN lbrynet.stream_blob sb ON lb.blob_hash = sb.blob_hash
                INNER JOIN main.blob mb ON mb.blob_hash = lb.blob_hash
            GROUP BY sb.stream_hash
            ORDER BY max_seed_count ASC, max_seed_time ASC
            LIMIT 10;"""):
            pass
        db.execute("COMMIT;")

