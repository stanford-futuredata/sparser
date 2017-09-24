#! /usr/bin/env python

import sys
import re
import numpy as np

query_dir = sys.argv[1]

with open(query_dir + 'read-only-hdfs.txt') as read_only_f:
    read_only_lines = read_only_f.read()
    read_only_timings = [
        float(x) for x in re.findall('Total Job Time: (.*)', read_only_lines)
    ]
    # Skip the first run, ignore JVM warmup time
    read_only_timings = read_only_timings[1:6]

with open('timings.csv', 'w') as w:
    print >> w, 'query,spark_time,sparser_time'
    print >> w, 'read-only,' + str(min(read_only_timings)) + ',,'
    for query in xrange(1, 9):
        query_str = query_dir + str(query)
        with open(query_str + '-spark-hdfs.txt') as spark_f, open(
                query_str + '-sparser-hdfs.txt') as sparser_f:
            spark_lines, sparser_lines = spark_f.read(), sparser_f.read()

            #read_hdfs_timings = [float(x) for x in re.findall('Read time: (.*)', lines)]
            #sparser_timings = [float(x) for x in re.findall('Total Runtime: (.*) seconds', lines)]
            #hdfs_sparser_timings = np.array(read_hdfs_timings) + np.array(sparser_timings)
            #cpp_timings = [float(x) for x in re.findall('Total Time in C\+\+: (.*)', lines)]

            spark_timings = [
                float(x)
                for x in re.findall('Total Job Time: (.*)', spark_lines)
            ]
            sparser_timings = [
                float(x)
                for x in re.findall('Total Job Time: (.*)', sparser_lines)
            ]
            # Skip the first run, ignore JVM warmup time
            spark_timings = spark_timings[1:6]
            sparser_timings = sparser_timings[1:6]
	    print >> w, '%s,%f,%f' % (query_str, min(spark_timings), min(sparser_timings))

        #assert (len(read_hdfs_timings) == len(sparser_timings) == len(cpp_timings)
        #        == len(spark_timings))
        #for a, b, c, d, e in zip(read_hdfs_timings, sparser_timings,
        #                         hdfs_sparser_timings, cpp_timings, spark_timings):
        #    print >> w, '%f,%f,%f,%f,%f' % (a, b, c, d, e)
