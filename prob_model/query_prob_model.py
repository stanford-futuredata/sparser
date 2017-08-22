#! /usr/bin/env python

from collections import defaultdict
import pickle
import argparse
from sys import maxint


def query_prob_model(prob_model, query, min_length=2, max_length=4):
    best_substr, lowest_count = None, maxint
    for i in xrange(len(query)):
        for j in xrange(min_length, min(len(query) - i + 1, max_length + 1)):
            substr = query[i:i + j]
            if prob_model[substr] < lowest_count:
                best_substr = substr
                lowest_count = prob_model[substr]
    return best_substr


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--prob-model-file', required=True)
    parser.add_argument('--query', required=optinal)
    args = parser.parse_args()

    with open(args.prob_model_file, 'b') as f:
        prob_model = pickle.load(f)
    best_substr = query_prob_model(prob_model, args.query)
    print('Query: %s, best substring: %s' % (args.query, best_substr))


if __name__ == '__main__':
    main()
