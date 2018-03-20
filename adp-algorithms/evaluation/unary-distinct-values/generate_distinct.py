import sys
import random
import numpy as np

"""
Obtain distinct count and total count from program arguments and
output a CSV with two integer columns of same distinctness.
"""

count_distinct = int(sys.argv[1])
count_total = int(sys.argv[2])

f = open("distinct_%d_%d" % (count_distinct, count_total), "w")
f.write("a,b\n")

data1 = np.random.randint(0, count_distinct, count_total)
data2 = np.random.randint(0, count_distinct, count_total)

for item in zip(data1, data2):
    f.write("%d,%d\n" % item)

f.close()
