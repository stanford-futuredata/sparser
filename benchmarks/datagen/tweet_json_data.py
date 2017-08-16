#/usr/bin/env python

import os
import sys

root = os.environ["SPARSER_HOME"]
if root[-1] != "/":
    root += "/"
root += "benchmarks/datagen/"
source_data_path = root + "_data/tweets.json"

def download_source_data():
    if not os.path.exists(source_data_path):
        os.system("wget\
                https://s3-us-west-2.amazonaws.com/sparser-datasets/tweets.json\
                -O {}".format(source_data_path))

if __name__=="__main__":
    download_source_data()
    # TODO replication

