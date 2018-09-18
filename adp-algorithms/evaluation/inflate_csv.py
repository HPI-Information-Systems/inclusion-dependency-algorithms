#!/usr/bin/env python
import os
import sys
import pandas as pd


SEP = ";"
HEADER = None


def to_output(path, axis, i):
	parts = os.path.split(path)
	filename = parts[-1]
	name, ext = filename.rsplit(".", 1)
	label = ('rows', 'columns')[axis]
	
	output_dir = os.path.join(*parts[:-1], "inflate_" + label)
	if not os.path.exists(output_dir):
		os.mkdir(output_dir)

	return os.path.join(output_dir, "%s_%d.%s" % (name, i, ext))


def process(path, how_many, axis):
	frame = pd.read_csv(path, sep=SEP, header=HEADER, escapechar='\\')

	for i in range(2, how_many + 2):
		print("Processing fold", i)
		df = pd.concat([frame] * i, axis=axis, ignore_index=True)
		output = to_output(path, axis, i)
		df.to_csv(output, index=False, header=None, sep=SEP)
		print("Written", output)


if __name__ == '__main__':
	filename = sys.argv[1]
	how_many = int(sys.argv[2])
	axis = int(sys.argv[3])
	process(filename, how_many=how_many, axis=axis)
