import apsw
import datetime
#from lbry.conf import Config
import math
import threading
import time

# TODO: Use conf data_dir
DB_FILENAME = "/home/brewer/.local/share/lbry/lbrynet/data_stats.db"
DATA_STATS_ENABLED = True
INV_WEEK = 1.0/(7*86400)

class DataStats:
    """
    Manages connections to the data_stats database.
    TODO: Consult with Jack on how to use asyncio instead of having a different
          connection for each function.
    """

    def __init__(self):
        self.lock = threading.Lock()
        self.conn = apsw.Connection(DB_FILENAME)
        self.conn.createscalarfunction("exp", math.exp, 1)
        self.db = self.conn.cursor()
        self.setup_database()

    def __del__(self):
        self.conn.close()

    def setup_database(self):
        self.lock.acquire()
        self.db.execute("PRAGMA SYNCHRONOUS = 0;")
        self.db.execute("PRAGMA JOURNAL_MODE = WAL;")
        self.db.execute("BEGIN;")

        self.db.execute("""
            CREATE TABLE IF NOT EXISTS blob
            (blob_hash      TEXT NOT NULL PRIMARY KEY,
             seed_count     INTEGER NOT NULL,
             last_seed_time REAL NOT NULL,
             popularity     REAL NOT NULL)
            WITHOUT ROWID;""")

        self.db.execute("""
            CREATE TABLE IF NOT EXISTS hour
            (start_time      INTEGER NOT NULL PRIMARY KEY,
             blobs_up        INTEGER NOT NULL,
             blobs_down      INTEGER NOT NULL,
             blobs_announced INTEGER NOT NULL)
            WITHOUT ROWID;""")

        self.db.execute("COMMIT;")
        self.lock.release()


    def log_seed(self, blob_hash):
        """
        Log that a particular blob was seeded.
        """
        # Time and date shenanigans
        now = time.time()
        hour = datetime.datetime.fromtimestamp(now)
        hour = hour.replace(minute=0, second=0, microsecond=0)

        self.lock.acquire()
        self.db.execute("BEGIN;")

        # Upsert
        self.db.execute(f"""
            INSERT INTO blob VALUES (?, 1, ?, 1.0)
            ON CONFLICT (blob_hash) DO UPDATE
            SET
                seed_count = seed_count + 1,
                popularity = popularity*exp(-{INV_WEEK}*(excluded.last_seed_time - blob.last_seed_time)) + 1.0,
                last_seed_time = excluded.last_seed_time;""", (blob_hash, now))

        # Update hour table
        self.db.execute("""
            INSERT INTO hour VALUES (?, 1, 0, 0)
            ON CONFLICT (start_time)
            DO UPDATE SET blobs_up = blobs_up + 1;
            """, (int(hour.timestamp()), ))

        self.db.execute("COMMIT;")
        self.lock.release()


    def log_download(self):
        """
        Log that a blob was downloaded.
        """
        now = time.time()
        hour = datetime.datetime.fromtimestamp(now)
        hour = hour.replace(minute=0, second=0, microsecond=0)

        # Update hour table
        self.lock.acquire()
        self.db.execute("BEGIN;")
        self.db.execute("""
            INSERT INTO hour VALUES (?, 0, 1, 0)
            ON CONFLICT (start_time)
            DO UPDATE SET blobs_down = blobs_down + 1;
            """, (int(hour.timestamp()), ))
        self.db.execute("COMMIT;")
        self.lock.release()



    def log_announcement(self):
        """
        Log that a blob was announced
        """
        now = time.time()
        hour = datetime.datetime.fromtimestamp(now)
        hour = hour.replace(minute=0, second=0, microsecond=0)

        # Update hour table
        self.lock.acquire()
        self.db.execute("BEGIN;")
        self.db.execute("""
            INSERT INTO hour VALUES (?, 0, 0, 1)
            ON CONFLICT (start_time)
            DO UPDATE SET blobs_announced = blobs_announced + 1;
            """, (int(hour.timestamp()), ))
        self.db.execute("COMMIT;")
        self.lock.release()

def find_unpopular_streams():
    """
    Find the least-seeded streams (recently)
    """
    # Grab current time
    now = time.time()

    # Read-only connection
    conn = apsw.Connection(DB_FILENAME, flags=apsw.SQLITE_OPEN_READONLY)
    db = conn.cursor()
    db.execute("ATTACH DATABASE '/home/brewer/.local/share/lbry/lbrynet/lbrynet.sqlite' AS lbrynet;")

    # Map from stream hash to popularity of most popular blob in the stream
    stream_popularities = {}

    # Get *current* blobs and popularities
    for row in db.execute("""
        SELECT sb.stream_hash, lb.blob_hash, mb.last_seed_time, mb.popularity
            FROM
            lbrynet.blob lb
            INNER JOIN lbrynet.stream_blob sb ON lb.blob_hash = sb.blob_hash
            INNER JOIN main.blob mb ON mb.blob_hash = lb.blob_hash
        LIMIT 10;"""):
        stream_hash, blob_hash, last_seed_time, popularity = row

        # Decay popularity
        popularity *= math.exp(-INV_WEEK*(now - last_seed_time))

        # Insert if necessary
        if stream_hash not in stream_popularities:
            stream_popularities[stream_hash] = 0.0

        if popularity > stream_popularities[stream_hash]:
            stream_popularities[stream_hash] = popularity

    # Sort in ascending order of popularity. Result is a list of tuples
    result = []
    for stream_hash in stream_popularities:
        result.append((stream_hash, stream_popularities[stream_hash]))
    result = sorted(result, key = lambda x: x[1])

    return result

