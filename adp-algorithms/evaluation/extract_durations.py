import os
import re
import sys

"""
Given a directory or file file list, extract the duration from a metanome-cli
logfile. If the filename of the log contains any digits, the first group
is taken as sort criterion (= sinddX.log, where X is the row /column count,
the Xth run, etc.).
Finally the durations are printed as comma-delimited string.
"""

def lines(f):
	with open(f) as w:
		return w.readlines()


DURATION = re.compile("\\((\\d+) ms\\)")


def extract(line):
	global DURATION
	match = DURATION.search(line)
	if match:
		return match.group(1)


arg = sys.argv[1]
stamps = []

files = []
for arg in sys.argv[1:]:
	if os.path.isdir(arg):
		for item in os.listdir(arg):
			if item.endswith('.log'):
				files.append((arg, item))
	else:
		if arg.endswith('.log'):
			head, tail = os.path.split(arg)
			files.append((os.path.abspath(head), tail))


COUNT = re.compile("(\\d+)")


def _key(pair):
	global COUNT
	directory, filename = pair
	match = COUNT.search(filename)
	if match:
		return int(match.group(0))
	else:
		return 0

files.sort(key=_key)
for f in files:
	print(f)

for f in files:
	full = os.path.join(*f)
	lastline = lines(full)[-1]
	match = extract(lastline)
	if match:
		stamps.append(match)

print(",".join(stamps))
