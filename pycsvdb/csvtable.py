import uuid
import traceback

TYPE_MAP = {
    str : 'text',
    int : 'int',
    float : 'real'
}

def get_type(data):
    try:
        int(data)
        return int
    except:
        pass

    try:
        float(data)
        return float
    except:
        pass

    return str

class Table(object):
    def __init__(self, row, name=None, colnames=None):
        self.types = tuple([get_type(x) for x in row])
        self.name = name or str(uuid.uuid4())
        self.colnames = colnames if colnames else tuple([self.name + '_' + str(i)
            for i in xrange(len(self.types))])
        self.dirty = True

    def __str__(self):
        return "<" + self.name + " : " + str(self.colnames) + ">"

    def mark_dirty(self):
        self.dirty = True

    def mark_clean(self):
        self.dirty = False

    def create_string(self):
        type_str = '('
        for i in xrange(len(self.types)):
            type_str += '{} {},'.format(self.colnames[i],
                TYPE_MAP[self.types[i]])
        type_str = type_str.strip(',')
        type_str += ')'

        return 'CREATE TABLE {} {}'.format(self.name, type_str)

    def create_table(self, cur):
        cur.execute(self.create_string())

    def valid_tup(self, tup):
        if len(tup) != len(self.types):
            return False

        for i,v in enumerate(tup):
            if get_type(v) != self.types[i]:
                return False

        return True

    def typed_tuple(self, tup):
        return tuple(self.types[i](v) for i,v in enumerate(tup))

    def insert_tuples(self, cur, batch):
        sql = ['INSERT INTO {} VALUES'.format(self.name)]
        for tup in batch:
            # checking kills performance
            # if not self.valid_tup(tup):
            #     raise ValueError("Invalid tuple " + str(tup))
            sql.append(str(self.typed_tuple(tup)) + ',')

        if len(sql) == 0:
            return

        # get rid of trailing comma
        sql[-1] = sql[-1][:-1]
        sql = ' '.join(sql)
        cur.execute(sql)

    def insert_tuple(self, cur, tup):
        if not self.valid_tup(tup):
            raise ValueError("Invalid tuple " + str(tup))
        cur.execute('INSERT INTO {} VALUES {}'.format(self.name, self.typed_tuple(tup)))

