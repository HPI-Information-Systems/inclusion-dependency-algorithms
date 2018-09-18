#!/usr/bin/env python

import pandas as pd
import sys
import os
import os.path

"""
Split CSVs that exceed the internal column limit of Postgres of 1600 columns per relation.
"""

THRESHOLD = 1600  # max. column count on Postgres
HEADER = True


def _read_csv(path, *args, **kwargs):
	return pd.read_csv(path,
		sep=',',
		header=True if HEADER else None,
		escapechar='\\',
		#engine='python',
		*args,
		**kwargs)


def should_split(path):
	frame = _read_csv(path, nrows=1)
	column_count = len(frame.columns)
	#print("Read frame %s, column count %d" % (path, column_count))
	return column_count > THRESHOLD


def to_output_name(path, index):
	parts = os.path.split(path)
	name = parts[-1]
	filename, ext = name.rsplit('.', 1)
	new_filename = "%s_%d.%s" % (filename, index, ext)
	return os.path.join(*parts[:-1], new_filename)


def split(path):
	frame = _read_csv(path)
	column_count = len(frame.columns)
	check = 0
	written = 0

	for i in range(0, column_count, THRESHOLD):
		lower = i
		upper = min(column_count, lower + THRESHOLD)

		#print("Processing columns from %d to %d" % (lower, upper))

		part = frame.iloc[:, lower:upper]
		check += len(part.columns)

		#print("Slicing completed, writing...")

		output = to_output_name(path, written)
		written += 1
		part.to_csv(output, index=False, header=None)
		print(output)

	assert column_count == check, "Columns written (%d) should match columns read (%d)" % (check, column_count)


if __name__ == '__main__':

	if len(sys.argv) < 2:
		print("Usage: splitcsv.py <list of files>")

	for item in sys.argv[1:]:

		if not item.endswith('.csv'):
			continue

		if should_split(item):
			#print('Splitting %s' % item)
			split(item)
