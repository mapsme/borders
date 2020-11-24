#!/usr/bin/python3
import logging
import sys
import time

import psycopg2

import config

try:
    from daemon import runner
    HAS_DAEMON = True
except:
    HAS_DAEMON = False


borders_table = config.BORDERS_TABLE

CONNECT_WAIT_INTERVAL = 5
CHECK_BORDERS_INTERVAL = 10

# For geometries crossing 180th meridian envelope area calculates to
# very small values. Examples area 'United States', 'Chukotka Autonomous Okrug',
# 'Alaska', 'Tuvalu'. For those borders area > env_area.
# Limit on envelope area is imposed due to countries whose bbox covers half the world
# like France or Netherlands with oversea territories for which tile counting
# lasts too long.
no_count_queries = [
    f"""
        SELECT id, name
        FROM (
            SELECT id, name,
            ST_Area(geography(geom))/1000000.0 area,
            ST_Area(geography(ST_Envelope(geom)))/1000000.0 env_area
            FROM {borders_table}
            WHERE {condition}) q
        WHERE area != 'NaN'::double precision
            AND area <= env_area
            AND env_area < 5000000
        -- ORDER BY area  -- makes the query too much slower (why?)
        LIMIT 1
    """ for condition in ('count_k < 0', 'count_k IS NULL')
]

class App():
    def __init__(self):
        self.stdin_path = '/dev/null'
        self.stdout_path = '/dev/tty'
        self.stderr_path = '/dev/tty'
        self.pidfile_path = config.DAEMON_PID_PATH
        self.pidfile_timeout = 5
        self.conn = None

    def get_connection(self):
        while True:
            try:
                if self.conn is None or self.conn.closed:
                    self.conn = psycopg2.connect(config.CONNECTION)
                    self.conn.autocommit = True
                
                with self.conn.cursor() as cur:
                    cur.execute(f"SELECT count_k FROM {borders_table} LIMIT 1")
                
                return self.conn
            except psycopg2.Error:
                try:
                   self.conn.close()
                except:
                   pass
                time.sleep(CONNECT_WAIT_INTERVAL)

    def process(self, region_id, region_name):
        msg = f'Processing {region_name} ({region_id})'
        logger.info(msg)
        try:
            f = open(config.DAEMON_STATUS_PATH, 'w')
            f.write(msg)
            f.close()
        except Exception as e:
            logger.error(str(e))
            pass

        with self.get_connection().cursor() as cur:
            cur.execute(f"""
                UPDATE {borders_table}
                SET count_k = n.count
                FROM (SELECT coalesce(sum(t.count), 0) AS count
                      FROM {borders_table} b, tiles t
                      WHERE b.id = %s AND ST_Intersects(b.geom, t.tile)
                     ) AS n
                WHERE id = %s
                """, (region_id, region_id)
            )
        try:
            f = open(config.DAEMON_STATUS_PATH, 'w')
            f.close()
        except Exception as e:
            logger.error(str(e))
            pass

    def find_region(self):
        with self.get_connection().cursor() as cur:
            cur.execute(no_count_queries[0])
            res = cur.fetchone()
            if not res:
                cur.execute(no_count_queries[1])
                res = cur.fetchone()
        return res if res else (None, None)

    def run(self):
        while True:
            try:
                region_id, region_name = self.find_region()
                if region_id:
                    self.process(region_id, region_name)
                else:
                    time.sleep(CHECK_BORDERS_INTERVAL)
            except:
                time.sleep(CHECK_BORDERS_INTERVAL)

def init_logger():
    logger = logging.getLogger("borders-daemon")
    logger.setLevel(logging.INFO)
    formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
    handler = logging.FileHandler(config.DAEMON_LOG_PATH)
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    return logger

if __name__ == '__main__':
    app = App()
    logger = init_logger()
    if not HAS_DAEMON or (len(sys.argv) > 1 and sys.argv[1] == 'run'):
        app.run()
    else:
        r = runner.DaemonRunner(app)
        r.do_action()
