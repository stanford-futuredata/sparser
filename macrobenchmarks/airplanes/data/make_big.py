
import sys

# Approximate size in KB of the base file.
approx_size = 282
# Number of copies for 1GB of data.
gb_size = 1000000 / approx_size

if len(sys.argv) > 1:
    scale_factor = int(sys.argv[1]) * gb_size
else:
    scale_factor = gb_size

with open("airplanes_small.csv", "r") as f:
    data = f.read()

with open("airplanes_big.csv", "w") as f:
    for _ in xrange(scale_factor):
        f.write(data)
