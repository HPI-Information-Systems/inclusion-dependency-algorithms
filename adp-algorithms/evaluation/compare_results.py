#! /usr/bin/env python
# ! -*- encoding: utf-8 -*-
import json
import sys

"""
Compare two output files of Metanome.

Print information on how many INDs were found and print some example INDs which
were found in only one of the sets.
In order to compare output files of algorithms that were run on databases and
CSV files,

(1) the ".csv" suffix of relation names is truncated,
(2) the identifiers are compared case-insensitively.
"""


def ind_to_str(ind):
    assert ind['type'] == 'InclusionDependency'

    def _table(n):
        return n.rsplit(".", 1)[0] if n.endswith('.csv') else n

    def _join_columns(columns):
        names = [_table(col['tableIdentifier']) + '.' + col['columnIdentifier']
                 for col in columns]
        return ','.join(names)

    return _join_columns(ind['dependant']['columnIdentifiers']) + ' [= ' + \
        _join_columns(ind['referenced']['columnIdentifiers'])


def parse_ind(filename):
    with open(filename, 'r') as f:
        result = []
        for line in f.readlines():
            result.append(ind_to_str(json.loads(line)).lower())
        return result


def print_info(name, ind):
    print("File '{}' contains {} INDs".format(name, len(ind)))


def print_difference(file1, ind1, file2, ind2):
    diff = ind1 - ind2
    print("{} elements of '{}' are not in '{}'".format(len(diff), file1, file2))
    for item in list(diff)[:20]:
        print(item)


def main():
    if len(sys.argv) < 3:
        print("Usage: python compare_result.py file1 file2")
        exit()

    file1 = sys.argv[1]
    file2 = sys.argv[2]

    ind1 = set(parse_ind(file1))
    ind2 = set(parse_ind(file2))

    print_info(file1, ind1)
    print_info(file2, ind2)

    print('')
    print_difference(file1, ind1, file2, ind2)
    print('')
    print_difference(file2, ind2, file1, ind1)


if __name__ == '__main__':
    main()
