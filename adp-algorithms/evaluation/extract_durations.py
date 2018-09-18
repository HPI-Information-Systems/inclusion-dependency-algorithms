import os
import os.path
import re
import sys
from collections import defaultdict

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
COUNT = re.compile("(\\d+)")


def extract(line):
	global DURATION
	match = DURATION.search(line)
	if match:
		return match.group(1)


def collect_logfiles(path):
	if os.path.isdir(path):
		files = [os.path.join(path, f) for f in os.listdir(path)]
	else:
		files = [path]

	return [f for f in files if f.endswith('.log')]


def parse_runtime(logfile):
	lastline = lines(logfile)[-1]
	return extract(lastline)


def collect_runtimes(logfiles):
	logfiles = collect_logfiles(logfiles)
	return [(logfile, parse_runtime(logfile)) for logfile in logfiles]


def process(batch):

	def sort_key(pair):
		global COUNT
		logfile, _ = pair
		filename = os.path.split(logfile)[-1]
		match = COUNT.search(filename)
		if match:
			return int(match.group(0))
		return 0

	runtimes = collect_runtimes(batch)
	runtimes.sort(key=sort_key)
	return runtimes


def write_output(dataset, all_runtimes):
	output_directory = 'parsed-output'
	if not os.path.exists(output_directory):
		os.mkdir(output_directory)

	filename = os.path.join(output_directory, '%s.csv' % dataset)
	print("Writing %s" % filename)

	max_runtime_count = max(len(x) for x in all_runtimes.values())

	def to_line(runtimes):
		numbers = [n if n is not None else '' for _, n in runtimes]
		padded = numbers + [''] * (max_runtime_count - len(numbers))
		return ",".join(padded)

	with open(filename, 'w') as f:
		for algorithm, runtimes in all_runtimes.items():
			f.write("%s,%s\n" % (algorithm, to_line(runtimes)))


def algorithm_name_key(path):
	# Assuming "dataset-algorithm" (e.g. "scop-binder")
	lastdir = os.path.split(path)[-1]
	_, algorithm = lastdir.split('-', 1)
	return algorithm


def filename_key(path):
	return os.path.split(path)[-1]


def main(dataset, paths, key_func=algorithm_name_key):
	all_runtimes = {}

	for path in paths:
		print('Processing %s' % path)
		runtimes = process(path)
		for runtime in runtimes:
			print(runtime)
		print()
		key = key_func(path)
		all_runtimes[key] = runtimes

	write_output(dataset, all_runtimes)


def dataset_runtime_main():
	to_search = sys.argv[1]
	files = defaultdict(lambda: [])

	for path in os.listdir(to_search):
		full = os.path.join(to_search, path)

		if not os.path.isdir(full):
			continue

		parts = path.split("-", maxsplit=1)
		if len(parts) < 2:
			continue

		dataset = parts[0]
		files[dataset].append(os.path.join(full))

	for key, group in files.items():
		print(key)
		print(group)
		print()

		main(key, group)


if __name__ == '__main__':
	#dataset_runtime_main()
	main("rowcount-musicbrainz", paths=sys.argv[1:], key_func=filename_key)
