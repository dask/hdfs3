#!/usr/bin/env python

import hdfs3
from collections import defaultdict, Counter
from toolz import topk

hdfs = hdfs3.HDFileSystem('NAMENODE_HOSTNAME', port=NAMENODE_PORT)

filenames = hdfs.glob('/tmp/enron/*/*')

print(filenames[:5])
print(hdfs.head(filenames[0]))

def count_words(file):
    word_counts = defaultdict(int)
    for line in file:
        for word in line.split():
            word_counts[word] += 1
    return word_counts

print(count_words(['apple banana apple', 'apple orange']))

with hdfs.open(filenames[0]) as f:
    counts = count_words(f)

print(topk(10, counts.items(), key=lambda k_v: k_v[1]))

all_counts = Counter()
for fn in filenames:
    with hdfs.open(fn) as f:
        counts = count_words(f)
        all_counts.update(counts)

print(len(all_counts))

print(topk(10, all_counts.items(), key=lambda k_v: k_v[1]))
