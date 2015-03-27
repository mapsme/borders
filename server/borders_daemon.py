#!/usr/bin/python
import os, sys
import time
import logging
import psycopg2
import config
try:
	from daemon import runner
	HAS_DAEMON = True
except:
	HAS_DAEMON = False

class App():
	def __init__(self):
		self.stdin_path = '/dev/null'
		self.stdout_path = '/dev/tty'
		self.stderr_path = '/dev/tty'
		self.pidfile_path = '/var/run/borders-daemon.pid'
		self.pidfile_timeout = 5

	def process(self, region):
		logger.info('Processing {}'.format(region))
		try:
			f = open(config.DAEMON_STATUS_PATH, 'w')
			f.write(region)
			f.close()
		except:
			pass

		with self.conn.cursor() as cur:
			cur.execute('update {table} set count_k = n.count from (select coalesce(sum(t.count), 0) as count from {table} b, tiles t where ST_Intersects(b.geom, t.tile) and name = %s) as n where name = %s;'.format(table=config.TABLE), (region, region));
		try:
			f = open(config.DAEMON_STATUS_PATH, 'w')
			f.close()
		except:
			pass

	def find_region(self):
		with self.conn.cursor() as cur:
			cur.execute('select name from {table} where count_k < 0 order by st_area(geom) limit 1;'.format(table=config.TABLE))
			res = cur.fetchone()
			if not res:
				cur.execute('select name from {table} where count_k is null order by st_area(geom) limit 1;'.format(table=config.TABLE))
				res = cur.fetchone()
		return res[0] if res else None

	def run(self):
		self.conn = psycopg2.connect(config.CONNECTION)
		self.conn.autocommit = True
		while True:
			region = self.find_region()
			if region:
				self.process(region)
			time.sleep(1) # todo: 10

def init_logger():
	logger = logging.getLogger("borders-daemon")
	logger.setLevel(logging.INFO)
	formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
	#handler = logging.FileHandler("/var/log/borders-daemon.log")
	handler = logging.StreamHandler()
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
