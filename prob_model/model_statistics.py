#! /usr/bin/env python

from collections import defaultdict
import itertools
import pickle
import argparse
import sys

def parse_query(query):
    """
    Returns a list of query strings. Each query string
    must pass for the predicate to pass
    (i.e., the query represents a conjunction).
    """
    return query.strip().split(",")

def add_to_model(model, query, body, min_length=2, max_length=4):
    """
    Searches for each substring of query in body, with the given min and max length.
    """
    min_length = max(0, min_length)

    for i in xrange(min_length, max_length + 1):
        if i not in model[query]:
            model[query][i] = defaultdict(int)

    for i in xrange(len(query)):
        for j in xrange(min_length, min(len(query) - i + 1, max_length + 1)):
            substr = query[i:i + j]
            model[query][j][substr] += body.count(substr)

def lowest_count(d):
    smallest = sys.maxint
    for x in d:
        if d[x] < smallest:
            smallest = x
    return smallest

class Statistics:
    def __init__(self):
        self.false_positives = 0
        self.success = 0
        self.not_found = 0
        self.cost = 0

def compute_statistics(model, corpus):
    """ Returns statistics about the query given the model and corpus."""

    # Maps search query -> statistics
    stats = defaultdict(Statistics)

    all_substrings = []
    for query in model:
        query_substrs = []
        for lengths in model[query]:
            query_substrs += model[query][lengths].keys()
        all_substrings.append(query_substrs)

    for line in corpus:
        # Check if the query will pass for the current line.
        passes = True
        for query in model:
            if query not in line:
                passes = False
                break

        # record false positives/successes for individual substrings
        for (query, substr_lengths) in model.iteritems():
            for (length, substrs) in substr_lengths.iteritems():
                # for individual substrings, only count the lowest occuring one.
                substr = lowest_count(substrs)
                stats[substr].cost = length
                if substr in line:
                    if not passes:
                        # False positive
                        stats[substr].false_positives += 1
                    else:
                        stats[substr].success += 1
                else:
                    stats[substr].not_found += 1

        # Find the statistics for pairwise and triplets of substrings.
        for iter1, iter2 in itertools.combinations(all_substrings, 2):
            for s0, s1 in itertools.product(iter1, iter2):
                print s0, s1
                key = "{}+{}".format(s0, s1)
                stats[key].cost = len(s0) + len(s1)
                if s0 in line and s1 in line:
                    if passes:
                        stats[key].false_positives += 1
                    else:
                        stats[key].success += 1
                else:
                    stats[key].not_found += 1
    return stats

def format_stats(stats):
    string = "Query\tFalse Positives\tSuccesses\tNot Found\tCost\n"
    for query in stats:
        stat = stats[query]
        string += "\t".join([query,
            str(stat.false_positives),
            str(stat.success),
            str(stat.not_found),
            str(stat.cost)])
        string += "\n"

    return string

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--corpus', required=True)
    parser.add_argument('--query', required=True)
    args = parser.parse_args()

    # { query -> {substring length -> {substring -> count}}}
    model = {}
    queries = parse_query(args.query)

    for query in queries:
        model[query] = {}

    full_corpus = []
    with open(args.corpus) as f:
        for line in f:
            for query in queries:
                add_to_model(model, query, line)
            full_corpus.append(line)

    stats = compute_statistics(model, full_corpus)
    print format_stats(stats)

if __name__ == '__main__':
    main()
