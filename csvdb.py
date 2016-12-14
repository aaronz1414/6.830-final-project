import sqlite3 as sqlite
import csv, os, time

from csvtable import Table
from monitor import Monitor

EXT = '.csv'
DROP_STATEMENT = 'DROP TABLE IF EXISTS {};'

class CSVDB(object):
    def __init__(self, filedir):
        try:
            os.remove('csvdb.db')
        except:
            pass

        self.monitor = Monitor(filedir.rstrip('/'), self._file_changed)
        self.tables = dict()
        self.table_paths = dict()
        self.conn = None
        self.cur = None

    def start(self):
        self.conn = sqlite.connect('csvdb.db')
        self.cur = self.conn.cursor()
        self.monitor.run()
        self.create_db()

    def stop(self):
        self.monitor.stop()

    def _file_changed(self, filename, path, mod_time):
        print '\n', filename_or_names, mod_time

        table_name = CSVDB.to_table_name(filename)
        if table_name not in self.tables:
            self.create_table(table_name, path)
        else:
            self.tables[table_name].mark_dirty()

    def create_table(self, table_name, path):
        self.table_paths[table_name] = path
        self.cur.execute(DROP_STATEMENT.format(table_name))
        reader = self.get_reader(table_name)
        for r in reader:
            self.tables[table_name] = Table(r, name=table_name)
            self.tables[table_name].create_table(self.cur)
            break
        self.conn.commit()

    def create_db(self):
        paths = self.monitor.get_abs_paths()
        files = self.monitor.get_files()
        for i,f in enumerate(files):
            self.create_table(CSVDB.to_table_name(f), paths[i])

    def update_db(self):
        for table_name, table in self.tables.iteritems():
            if table.dirty:
                self.cur.execute(DROP_STATEMENT.format(table_name))
                self.create_table(table_name, self.table_paths[table_name])
                reader = self.get_reader(table_name)
                for r in reader:
                    self.table.insert_tuple(self.cur, r)
                self.conn.commit()

    def get_reader(self, table_name):
        return csv.reader(self.table_paths[table_name], delimiter=',')

    @staticmethod
    def to_table_name(f):
        return f.strip(EXT)

def main(args):
    print "CSVDB Console (directory='{}')".format(args.csvdir)
    db = CSVDB(args.csvdir)
    db.start()
    print 'Database started'
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
    import argparse
    parser = argparse.ArgumentParser(description="CSVDB")
    parser.add_argument("--csvdir", default="data")
    args = parser.parse_args()
    main(args)
