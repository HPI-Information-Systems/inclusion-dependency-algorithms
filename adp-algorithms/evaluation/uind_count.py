#!/usr/bin/env python
import sys
import os
import re
import subprocess

files = sys.argv[1:]
count = re.compile("(\d+)")


def sort_key(f):
	return int(count.findall(f)[0])

files = sorted(files, key=sort_key)


def line_count(f):
	with open(f) as lines:
		return sum(1 for _ in lines)


ind_counts = [line_count(f) for f in files]
print(",".join(str(x) for x in ind_counts))