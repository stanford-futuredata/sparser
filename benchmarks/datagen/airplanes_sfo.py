
import numpy as np
import sys
import os

import argparse

root = os.environ["SPARSER_HOME"]
if root[-1] != "/":
    root += "/"
root += "benchmarks/datagen/"

def generate_data(scale, filename):
    with open(root + "sources/airplanes_sfo.csv", "r") as f:
        data = f.read()
    with open(root + filename, "w") as f:
        for _ in xrange(scale):
            f.write(data)

if __name__=="__main__":
    parser = argparse.ArgumentParser(description='Inflates the airplanes_sfo.csv dataset/')
    parser.add_argument('-s', '--scale', metavar='scale', type=int, required=True, nargs=1,
            help='Scale factor of the dataset.')
    parser.add_argument('-o', '--out', metavar='out', type=str, default=["_data/airplanes_big.csv"], nargs=1, 
           help='Output filename.')

    args = parser.parse_args()
    scale = args.scale[0]
    out = args.out[0]
    generate_data(scale, filename=out)

