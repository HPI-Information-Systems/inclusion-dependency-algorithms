#! /usr/bin/env python
import psycopg2
import sys
import os.path
import pandas as pd

"""
Load a CSV file into Postgres.

Dependencies:
psycopg2
pandas

Procedure:
With help of Pandas determine the column count of the CSV file. Then create a
table (name equals filename without ".csv" suffix) with columns "columnX" with
text data type. Then use Postgres facilities to load the CSV.

Given a single file, that file is loaded. Given a directory, all files inside
the directory are loaded. Empty files are ignored in both cases.
"""

SEP = ','


def get_connection():
    with open('pgpass.conf') as f:
        line = f.read()
        host, port, database, user, pw = line.strip().split(":")
        return psycopg2.connect(host=host, port=port, database=database, user=user, password=pw)


def get_columns(filename):
    global SEP
    frame = pd.read_csv(filename,
         nrows=1,
         sep=SEP,
         header=None,
         escapechar="\\", engine='python')
    return ['column' + str(index + 1) for index in range(len(frame.columns))]


def load(csv, cursor):
    global SEP
    columns = get_columns(csv)
    fname = os.path.basename(csv)
    relation = fname.rsplit(".", 1)[0] if fname.endswith(".csv") else fname
    relation = relation.lower()
    print("Loading", relation)
    typed = ", ".join(name + " TEXT" for name in columns)
    create = """CREATE TABLE "{name}" ({columns})""".format(name=relation, columns=typed)
    print(create)
    cursor.execute(create)

    copy = """COPY "{name}" FROM '{csv}' DELIMITER '{sep}' CSV ESCAPE '\\'""".format(name=relation, csv=os.path.abspath(csv), sep=SEP)

    with open(csv, 'r') as f:
        cursor.copy_expert(copy, f)


def main():
    if len(sys.argv) < 2:
        print("Usage: loadcsv.py <files or directory>")
        exit()

    connection = get_connection()
    cursor = connection.cursor()

    arg = sys.argv[1]
    if os.path.isdir(arg):
        files = [os.path.join(arg, f) for f in os.listdir(arg)]
    else:
        files = sys.argv[1:]

    for f in files:
        if not os.path.isdir(f) and os.path.getsize(f) > 0:
            load(f, cursor)

    connection.commit()
    cursor.close()
    connection.close()


if __name__ == "__main__":
    main()
