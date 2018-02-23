import os
import os.path
import sys


"""
Output a list of non-empty files to stdout, ".csv" suffix stripped.

The files inside the directory are supposed to be loaded into the database
first. Then this script can be used to determine valid relation names which
can be fed into Metanome CLI as part of the "load:" statement.
"""


def main(path):

    def _valid(p):
        return not os.path.isdir(p) and os.path.getsize(p) > 0

    files = os.listdir(path)
    nonempty = [x for x in files if _valid(os.path.join(path, x)) > 0]
    noendings = [x.rsplit(".")[0] if x.endswith(".csv") else x for x in nonempty]
    for x in noendings:
        print(x)


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: create_listing.py <directory>")
        exit()
    main(sys.argv[1])
