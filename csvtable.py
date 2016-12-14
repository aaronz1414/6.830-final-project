import uuid

TYPE_MAP = {
    str : 'text',
    int : 'real',
    float : 'real'
}

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
    def __init__(self, row, name=None, colnames=None):
        self.types = tuple([get_type(x) for x in row])
        self.name = name or str(uuid.uuid4())
        self.colnames = colnames if colnames else tuple([self.name + '_' + str(i)
            for i in xrange(len(self.types))])
        self.dirty = False

    def __str__(self):
        return "<" + self.name + " : " + str(self.colnames) + ">"

    def mark_dirty(self):
        self.dirty = False

    def mark_clean(self):
        self.dirty = True

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

    def insert_tuple(self, cur, tup):
        if len(tup) != len(self.types):
            raise ValueError("Invalid tuple " + str(tup))

        cur.execute('INSERT INTO {} VALUES {}'.format(self.name, tuple(tup)))
