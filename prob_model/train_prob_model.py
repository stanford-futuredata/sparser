#! /usr/bin/env python

from collections import defaultdict
import pickle
import argparse
from query_prob_model import query_prob_model

substrs_counts = defaultdict(int)


def add_to_model(token, min_length=2, max_length=4):
    for i in xrange(len(token)):
        for j in xrange(min_length, min(len(token) - i + 1, max_length + 1)):
            substr = token[i:i + j]
            substrs_counts[substr] += 1


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--training-file', required=True)
    parser.add_argument('--output-file', required=True)
    parser.add_argument('--query', required=False)
    args = parser.parse_args()

    with open(args.training_file) as f:
        for line in f:
            for token in line.split(' '):
                add_to_model(token)

    with open(args.output_file, 'wb') as f:
        pickle.dump(substrs_counts, f)

    if args.query:
        best_substr = query_prob_model(substrs_counts, args.query)
        print('Query: %s, best substring: %s' % (args.query, best_substr))


if __name__ == '__main__':
    main()
