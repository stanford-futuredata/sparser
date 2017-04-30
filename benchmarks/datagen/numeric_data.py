#!/usr/bin/env python

#
# Creates random numeric CSV data.
#

import numpy as np
import sys
import os

import argparse

root = os.environ["SPARSER_HOME"]
if root[-1] != "/":
    root += "/"
root += "benchmarks/datagen/"

def generate_data(rows, columns, maxint, filename="data/numbers.csv"):
    numbers = np.random.rand(rows,columns) * maxint
    np.savetxt(root + filename, numbers, fmt="%d", delimiter=",")

if __name__=="__main__":
    parser = argparse.ArgumentParser(description='Generates fake numeric CSV data.')
    parser.add_argument('-l', '--lines', metavar='lines', type=int, required=True, nargs=1,
            help='number of lines to generate in the file')
    parser.add_argument('-c', '--cols', metavar='cols', type=int, required=True, nargs=1,
            help='number of columns to generate in the file')
    parser.add_argument('-m', '--maxval', metavar='maxval', type=int, required=True, nargs=1, 
           help='Max value generated')
    parser.add_argument('-o', '--out', metavar='out', type=str, default=["_data/numbers.csv"], nargs=1, 
           help='Output filename')

    args = parser.parse_args()

    rows = args.lines[0]
    columns = args.cols[0]
    maxint = args.maxval[0]
    out = args.out[0]
    generate_data(rows, columns, maxint=maxint, filename=out)
