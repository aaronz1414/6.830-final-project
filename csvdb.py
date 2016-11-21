import sqlite3 as sqlite
import csv, os

from csvtable import Table

def main():
	os.remove('csvdb.db')
	conn = sqlite.connect('csvdb.db')
	cur = conn.cursor()
	print "CSVDB console"
	filename = raw_input("Enter the path to your csv: ")
	with open(filename, 'r') as f:
		table = None
		reader = csv.reader(f, delimiter=',')
		for r in reader:
			if not table:
				table = Table(r, name=filename.strip('.csv'))
				table.create_table(cur)
				conn.commit()
			
			table.insert_tuple(cur, r)
		conn.commit()
	print 'done'
	test(table.name)

def test(name):
	conn = sqlite.connect('csvdb.db')
	cur = conn.cursor()
	for r in cur.execute('select * from {}'.format(name)):
		print r
	print 'test done'

if __name__ == '__main__':
	main()
