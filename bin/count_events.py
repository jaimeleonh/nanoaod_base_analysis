#!/bin/env python3

import os, ROOT, sys

if len(sys.argv) != 2:
    print("Usage: count_events.py folder_path")
    sys.exit()

folder = sys.argv[1]
files = [os.path.join(folder, file) for file in os.listdir(folder)]
df = ROOT.RDataFrame("Events", tuple(files))
print(df.Count().GetValue())