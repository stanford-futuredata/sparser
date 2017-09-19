#! /usr/bin/env python

import sys
import re
import numpy as np

with open(sys.argv[1]) as f, open(sys.argv[1] + '_timings.csv', 'w') as w:
    lines = f.read()
    read_hdfs_timings = [float(x) for x in re.findall('Read time: (.*)', lines)]
    sparser_timings = [float(x) for x in re.findall('Total Runtime: (.*) seconds', lines)]
    hdfs_sparser_timings = np.array(read_hdfs_timings) + np.array(sparser_timings)
    cpp_timings = [float(x) for x in re.findall('Total Time in C\+\+: (.*)', lines)]
    spark_timings = [float(x) for x in re.findall('Total Job Time: (.*)', lines)]

    print >> w, 'hdfs_time,sparser_time,hdfs_and_sparser_time,cpp_time,spark_time'
    assert(len(read_hdfs_timings) == len(sparser_timings) == len(cpp_timings) == len(spark_timings))
    for a, b, c, d, e in zip(read_hdfs_timings, sparser_timings, hdfs_sparser_timings, cpp_timings, spark_timings):
        print >> w, '%f,%f,%f,%f,%f' % (a, b, c, d, e)
    

