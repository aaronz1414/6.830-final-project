import csv
from csvtable import Table

def create_table(table):
	pass

def main():
	print "CSVDB console"
	filename = raw_input("Enter the path to your csv: ")
	with open(filename, 'r') as f:
		table = None
		reader = csv.reader(f, delimiter=',')
		for r in reader:
			table = Table(r)
			break

		print table.types
		# reader = csv.reader(f, delimiter=',')
		# for r in reader:



if __name__ == '__main__':
	main()
