#!/usr/bin/env ipython

import sys

directory_name1=sys.argv[1]
directory_name2=sys.argv[2]

import os, ROOT

folder1=directory_name1
folder2=directory_name2


files1=[os.path.join(folder1,file) for file in sorted(os.listdir(folder1))]
files2=[os.path.join(folder2,file) for file in sorted(os.listdir(folder2))]

df1=ROOT.RDataFrame("Events",tuple(files1))
events1=df1.Count().GetValue()
print("Events in",folder1,":",events1)
df2=ROOT.RDataFrame("Events",tuple(files2))
events2=df2.Count().GetValue()
print("Events in",folder2,":",events2)

if events1==events2:
  print("No differences between the two folders")
else:
  print("The number of events are different")

