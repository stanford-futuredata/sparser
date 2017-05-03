
import sys
import os

import argparse

root = os.environ["SPARSER_HOME"]
if root[-1] != "/":
    root += "/"
root += "benchmarks/datagen/"
source_data_path = root + "sources/reviews_Amazon_Instant_Video_5.json"

def download_source_data():
    if not os.path.exists(source_data_path):
        os.system("wget\
                http://snap.stanford.edu/data/amazon/productGraph/categoryFiles/reviews_Amazon_Instant_Video_5.json.gz\
                -O {}.gz".format(source_data_path))
        os.system("gunzip {}.gz".format(source_data_path))
 
def generate_data(scale, filename):
    with open(source_data_path, "r") as f:
        data = f.read()
    with open(root + filename, "w") as f:
        for _ in xrange(scale):
            f.write(data)

if __name__=="__main__":
    parser = argparse.ArgumentParser(description='Inflates the Amazon Intant Video JSON dataset.')
    parser.add_argument('-s', '--scale', metavar='scale', type=int, required=True, nargs=1,
            help='Scale factor of the dataset.')
    parser.add_argument('-o', '--out', metavar='out', type=str,
            default=["_data/amazon_video_reviews.json"], nargs=1, 
           help='Output filename.')

    download_source_data()

    args = parser.parse_args()
    scale = args.scale[0]
    out = args.out[0]
    generate_data(scale, filename=out)
