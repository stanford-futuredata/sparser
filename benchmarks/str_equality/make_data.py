
import random

lines = 50000000
with open("data.csv", "w") as f:
    for i in xrange(lines):
        f.write(str(i) + "\n")

