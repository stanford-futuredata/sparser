#! /usr/bin/env python

import sys
import re
import numpy as np

query_dir = sys.argv[1] + '/'

QUERIES = [('twitter', 4), ('zakir', 10)]

for query_dataset, num_queries in QUERIES:
    with open(query_dataset + '_spark_experiments.csv', 'w') as w:
        print >> w, 'query,spark_time,spark_std_dev,sparser_time,sparser_std_dev,query_only_time,query_only_std_dev'
        with open(query_dir + query_dataset +
                  '-read-only-hdfs.txt') as read_only_f:
            read_only_lines = read_only_f.read()
            read_only_timings = [
                float(x)
                for x in re.findall('Total Job Time: (.*)', read_only_lines)
            ]
            # Skip the first run, ignore JVM warmup time
            read_only_timings = read_only_timings[1:6]

            print >> w, 'disk load time,%f,%f,,,,' % (np.mean(read_only_timings),
                                                 np.std(read_only_timings))

            for query in xrange(1, num_queries + 1):
                query_str = query_dir + query_dataset + str(query)
                with open(query_str + '-spark-hdfs.txt') as spark_f, open(
                        query_str + '-sparser-hdfs.txt') as sparser_f, open(
                            query_str +
                            '-query-only-hdfs.txt') as query_only_f:
                    spark_lines, sparser_lines, query_only_lines = spark_f.read(
                    ), sparser_f.read(), query_only_f.read()

                    spark_timings = [
                        float(x)
                        for x in re.findall('Total Job Time: (.*)',
                                            spark_lines)
                    ]
                    sparser_timings = [
                        float(x)
                        for x in re.findall('Total Job Time: (.*)',
                                            sparser_lines)
                    ]
                    query_only_timings = [
                        float(x)
                        for x in re.findall('Query Time: (.*)',
                                            query_only_lines)
                    ]
                    # Skip the first run, ignore JVM warmup time
                    spark_timings = spark_timings[1:6]
                    sparser_timings = sparser_timings[1:6]
                    query_only_timings = query_only_timings[1:6]
                    print >> w, '%d,%f,%f,%f,%f,%f,%f' % (
                        query, np.mean(spark_timings),
                        np.std(spark_timings), np.mean(sparser_timings),
                        np.std(sparser_timings), np.mean(query_only_timings),
                        np.std(sparser_timings))
