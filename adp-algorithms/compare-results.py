#! /usr/bin/env python
# ! -*- encoding: utf-8 -*-
import json
import sys


def ind_to_str(ind):
    assert ind['type'] == 'InclusionDependency'

    def _join_columns(columns):
        names = [col['tableIdentifier'] + '.' + col['columnIdentifier']
                 for col in columns]
        return ','.join(names)

    return _join_columns(ind['dependant']['columnIdentifiers']) + ' [= ' + \
        _join_columns(ind['referenced']['columnIdentifiers'])


def parse_ind(filename):
    with open(filename, 'r') as f:
        result = []
        for line in f.readlines():
            result.append(ind_to_str(json.loads(line)))
        return result


def print_info(name, ind):
    print("File '{}' contains {} INDs".format(name, len(ind)))


def print_difference(file1, ind1, file2, ind2):
    diff = ind1 - ind2
    print("{} elements of '{}' are not in '{}'".format(len(diff), file1, file2))
    for item in list(diff)[:20]:
        print(item)


def main():
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
