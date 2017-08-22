#/usr/bin/env python

import os
import sys

root = os.environ["SPARSER_HOME"]
if root[-1] != "/":
    root += "/"
root += "benchmarks/datagen/_data/"
source_data_path = root + "tweets.json"


def download_source_data():

    if not os.path.exists(source_data_path):
        os.makedirs(root)
        os.system("wget\
                https://s3-us-west-2.amazonaws.com/sparser-datasets/tweets.json\
                -O {}".format(source_data_path))

if __name__=="__main__":
    download_source_data()
    # TODO replication

