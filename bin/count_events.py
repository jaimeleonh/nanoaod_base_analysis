#!/bin/env python3

import os, ROOT, sys, argparse

parser = argparse.ArgumentParser(
    prog="count_events.py", description="Count the number of events in files of a folder.\nUsage: count_events.py folder_path",
)
parser.add_argument("folder", help="Path to folder")
parser.add_argument("-f", "--first", help="Print only event count of the first file, instead of the sum of all files in directory", action="store_true")
args = parser.parse_args()

folder = args.folder
files = [os.path.join(folder, file) for file in os.listdir(folder)]
if args.first:
    files = files[0:1]
df = ROOT.RDataFrame("Events", tuple(files))
print(df.Count().GetValue())