import psycopg2
import os
import sys

"""
Get total count decrement step from arguments and
read DB credentials from pgpass.conf.
Then drop "step" columns from the table at once,
each step confirmed with an "enter" key press.
"""

def get_connection():
    with open('pgpass.conf') as f:
        line = f.read()
        host, port, database, user, pw = line.strip().split(":")
        return psycopg2.connect(host=host, port=port, database=database, user=user, password=pw)

def drop_columns(begin, end):
    for index in range(begin, end):
        sql = "alter table tesmaexp_temp drop column column{};".format(index)
        cmd = "psql -c \"{sql}\" cath".format(sql=sql)
        print(cmd)
        code = os.system(cmd)
        if code:
            print("something fishy")


def main():
    count = int(sys.argv[1])
    step = int(sys.argv[2])
    input("begin")
    for index in range(1, count, step):
        drop_columns(index, index + step)
        input()


if __name__ == '__main__':
    main()
