import sqlite3 as sqlite
import csv, os, time

from csvtable import Table
from monitor import Monitor

class CSVDB(object):
	def __init__(self, filename):
		try:
			os.remove('csvdb.db')
		except:
			pass
		self.filename = filename
		self.monitor = Monitor(self.filename, self._db_setup, self.update_db)
		self.file = open(self.filename, 'r')
		self.table = None

	def start(self):
		self.monitor.run()

	def stop(self):
		self.monitor.stop()

	def _db_setup(self):
		self.conn = sqlite.connect('csvdb.db')
		self.cur = self.conn.cursor()
		self.update_db()

	def update_db(self):
		self.cur.execute('DROP TABLE IF EXISTS ' + self.filename.strip('.csv') + ';')
		reader = csv.reader(self.file, delimiter=',')
		for r in reader:
			if not self.table:
				self.table = Table(r, name=self.filename.strip('.csv'))
				self.table.create_table(self.cur)
				self.conn.commit()

			self.table.insert_tuple(self.cur, r)
		self.conn.commit()

def main():
	print "CSVDB console"
	filename = raw_input("Enter the path to your csv: ")
	db = CSVDB(filename)
	db.start()
	print 'done'
	try:
		raw_input('waiting...')
	except:
		print 'terminating...'
	# test(db)
	db.stop()

def test(db):
	conn = sqlite.connect('csvdb.db')
	cur = conn.cursor()
	for r in cur.execute('select * from {}'.format(db.table.name)):
		print r
	print 'test done'

if __name__ == '__main__':
	main()
