#!/usr/bin/env python

import numpy as np
import pandas as pd
import sys
import os
import os.path

"""
Given a list of CSV files, output how many columns are empty in total and relative numbers.
"""

HEADER = False
SEP = ';'

def _read_csv(path, *args, **kwargs):
	return pd.read_csv(path,
		sep=SEP,
		header=1 if HEADER else None,
		escapechar='\\',

		# Below are mutually exclusive
		# Low memory supresses warnings about missing dtypes
		# Python engine may give better error message in case parsing fails
		low_memory=False,
		#engine='python',
		*args,
		**kwargs)


def columncount(filename):
	df = _read_csv(filename, nrows=1)
	return len(df.columns)


def emptycolumns(filename):
	df = _read_csv(filename)
	empty = 0.0
	for column in df.columns:
		if df[column].dropna().empty:
			empty += 1
	total = len(df.columns)
	percentage = int((empty / total) * 100)
	print("In {name}, {share}% of columns are empty ({empty} out of {total})".format(
		name=filename,
		share=percentage,
		empty=int(empty),
		total=total))
	return empty, total


def only_columncount():
	files = [x for x in sys.argv[1:] if os.path.getsize(x)]
	counts = []
	for f in files:
		counts.append((f, columncount(f)))
	counts.sort(key=lambda x: x[1])
	for count in counts:
		print("%d\t%s" % (count[1], count[0]))


def main():
	files = sys.argv[1:]
	empty = 0.0
	total = 0

	for f in files:
		file_empty, file_total = emptycolumns(f)
		empty += file_empty
		total += file_total

	percentage = int((empty / total) * 100)
	print("In total, {}% of columns are empty ({} out of {})".format(percentage, int(empty), total))


if __name__ == '__main__':
	#main()
	only_columncount()
