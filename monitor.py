import sqlite3 as sqlite
import csv, os, time

from threading import Thread
from csvtable import Table

class Monitor(object):

	def __init__(self):
		os.remove('csvdb.db')
		self.conn = sqlite.connect('csvdb.db')
		self.cur = self.conn.cursor()
		self.filename = ''
		self.table = None
		self.last_file_mod = 0

	def run(self):
		self.filename = raw_input("Enter the path to your csv: ")
		self.update_db()
		self.test(self.table.name)

		self.last_file_mod = os.stat(self.filename).st_mtime
		self.monitor_file_change_in_background()

	def update_db(self):
		start = time.time() * 1000
		self.cur.executescript('DROP TABLE IF EXISTS ' + self.filename.strip('.csv') + ';')
		self.table = None
		with open(self.filename, 'r') as f:
			reader = csv.reader(f, delimiter=',')
			for r in reader:
				if not self.table:
					self.table = Table(r, name=self.filename.strip('.csv'))
					self.table.create_table(self.cur)
					self.conn.commit()
				
				if len(r) == 0:
					continue
				self.table.insert_tuple(self.cur, r)
			self.conn.commit()

		elapsed = ((time.time() * 1000) - start)
		with open("result", 'w') as f:
			f.write(str(elapsed))

	def monitor_file_change_in_background(self):
		while True:
			if os.stat(self.filename).st_mtime > self.last_file_mod:
				self.last_file_mod = os.stat(self.filename).st_mtime
				self.update_db()
			time.sleep(1)


	def test(self, name):
		self.conn = sqlite.connect('csvdb.db')
		self.cur = self.conn.cursor()
		for r in self.cur.execute('select * from {}'.format(name)):
			# print r
			pass
		print 'test done'

monitor = Monitor()
monitor.run()