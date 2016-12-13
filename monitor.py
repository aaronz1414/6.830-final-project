import sqlite3 as sqlite
import csv, os, time

from threading import Thread
from csvtable import Table

class Monitor(object):
	def __init__(self, filename, setup, callback):
		self.filename = filename
		self.table = None
		self.last_file_mod = 0
		self.setup = setup
		self.callback = callback
		self.kill = False

	def run(self):
		self.last_file_mod = os.stat(self.filename).st_mtime
		Thread(target=self.monitor_file_change_in_background).start()

	def stop(self):
		self.kill = True

	def monitor_file_change_in_background(self):
		self.setup()
		while not self.kill:
			if os.stat(self.filename).st_mtime > self.last_file_mod:
				self.last_file_mod = os.stat(self.filename).st_mtime
				print '\nfile changed\n'
				self.callback()
				print 'back to waiting...'
			time.sleep(1)
