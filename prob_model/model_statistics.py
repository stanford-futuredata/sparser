#! /usr/bin/env python

from collections import defaultdict
import itertools
import pickle
import argparse
import sys

FULL_PARSE_COST = 25.

def parse_query(query):
    """
    Returns a list of query strings. Each query string
    must pass for the predicate to pass
    (i.e., the query represents a conjunction).
    """
    return query.strip().split(",")

lengths = [2,4,8]
def add_to_model(model, query, body):
    """
    Searches for each substring of query in body, with the given min and max length.
    """
    for i in lengths:
        if i not in model[query]:
            model[query][i] = defaultdict(int)

    for i in xrange(len(query)):
        for j in lengths:
            if i + j > len(query):
                continue
            substr = query[i:i + j]
            model[query][j][substr] += body.count(substr)

def lowest_count(d):
    smallest = sys.maxint
    ret = None
    for x in d:
        if d[x] < smallest:
            smallest = d[x]
            ret = x
    return ret

class Statistics:
    def __init__(self):
        self.false_positives = 0.
        self.success = 0.
        self.not_found = 0.
        self.cost = 0.

    @property
    def cost_index(self):
        return self.cost + self.false_positives / (self.success + self.false_positives) * FULL_PARSE_COST

    def __eq__(self, other):
        return self.cost_index == other.cost_index

    def __lt__(self, other):
        return self.cost_index < other.cost_index

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

        # Check each combination only once per line
        checked = set()

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

                if not substr:
                    continue
                if substr in checked:
                    continue
                checked.add(substr)

                stats[substr].cost = length
                stats[substr].key = substr
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

                if s0 == s1:
                    continue

                key = "{}+{}".format(s0, s1)
                
                if key in checked:
                    continue
                checked.add(key)

                stats[key].key = key
                stats[key].cost = len(s0) + len(s1)
                if s0 in line and s1 in line:
                    if not passes:
                        stats[key].false_positives += 1
                    else:
                        stats[key].success += 1
                else:
                    stats[key].not_found += 1
    return stats


def format_stats(stats):
    string = "Query\tFalse Positives\tSuccesses\tNot Found\tCost\tCost Index\n"
    stats = sorted(stats.values())
    for stat in stats:
        string += "\t".join([stat.key,
            str(stat.false_positives),
            str(stat.success),
            str(stat.not_found),
            str(stat.cost),
            str(stat.cost_index)])
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
