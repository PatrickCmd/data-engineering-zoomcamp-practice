import sys

import pandas

print(sys.argv)

day = sys.argv[1]

print(f"Job successfully finished for day = {day}.")

# Test run: docker run -it test:pandas 2022-04-10