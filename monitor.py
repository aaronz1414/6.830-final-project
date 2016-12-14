import sqlite3 as sqlite
import csv, os, time

from threading import Thread
from csvtable import Table

class Monitor(object):
    def __init__(self, filedir, callback, extension='.csv'):
        """Initialize monitor

        Args:
            filedir (str): the directory to monitor (no trailing slash)
            callback (func): the function that should be called
                when a file in filedir changes
            extension (str, optional): the extension of files to monitor
        """
        self.filedir = filedir
        self.callback = callback
        self.extension = extension
        self.kill = False

        self.file_mods = None

    def get_files(self):
        return filter(lambda f: self.extension in f, os.listdir(self.filedir))

    def _to_path(self, f):
        return self.filedir + '/' + f

    def get_abs_paths(self):
        return map(lambda f: self._to_path(f), self.get_files())

    def get_last_mod(self, f):
        return self.file_mods.get(f, None)

    def run(self):
        self.file_mods = {f: os.stat(self._to_path(f)).st_mtime for f in self.get_files()}
        Thread(target=self.monitor_directory).start()

    def stop(self):
        self.kill = True

    def monitor_directory(self):
        while not self.kill:
            for f in self.get_files():
                mod_time = os.stat(self._to_path(f)).st_mtime
                if f not in self.file_mods or mod_time > self.file_mods[f]:
                    self.file_mods[f] = mod_time
                    self.callback(f, self._to_path(f), mod_time)
            time.sleep(1)
