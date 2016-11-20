import uuid
import sqlite3

def get_type(data):
	try:
		float(data)
		return float
	except:
		pass

	try:
		int(data)
		return int
	except:
		pass

	return str

class Table(object):
	def __init__(self, row, name=None):
		tmptypes = list()
		for x in row:
			tmptypes.append(get_type(x))

		self.types = tuple(tmptypes)
		self.name = name or str(uuid.uuid4())
		self.colnames = tuple([self.name + '-' + str(i)
			for i in xrange(len(self.types))])

	def __str__(self):
		return "<" + self.name + " : " + str(self.colnames) + ">"