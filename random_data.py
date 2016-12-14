import random, string
import sys
import csv

def randomword():
    length = random.randint(5,75)
    return ''.join(random.choice(string.lowercase) for i in range(length))

def randomdouble():
    return random.randint(10,1000) * random.random()

def randomint():
    return random.randint(10,1000)

def main(args):
    filename = args[0]
    with open(filename, 'w') as f:
        writer = csv.writer(f, delimiter=',')
        for i in xrange(int(args[1])):
            writer.writerow((randomint(), randomdouble(), randomword()))
    print 'done'

if __name__ == '__main__':
    main(sys.argv[1:])