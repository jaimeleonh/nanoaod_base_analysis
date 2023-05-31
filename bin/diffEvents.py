#!/usr/bin/env ipython

import sys

directory_name1=sys.argv[1]
directory_name2=sys.argv[2]

import os, ROOT

folder1=directory_name1
folder2=directory_name2

files1=[os.path.join(folder1,file) for file in sorted(os.listdir(folder1))]
files2=[os.path.join(folder2,file) for file in sorted(os.listdir(folder2))]

for file1,file2 in zip(files1,files2):
    df1=ROOT.RDataFrame("Events",file1)
    events1=df1.Count().GetValue()
    df2=ROOT.RDataFrame("Events",file2)
    events2=df2.Count().GetValue()
    if events1!=events2:
       print("The number of events are different")
       print("Events in",file1,":",events1)
       print("Events in",file2,":",events2)
       print("Difference is:",abs(events1-events2))
    else:
       print("There is no difference between branches")

