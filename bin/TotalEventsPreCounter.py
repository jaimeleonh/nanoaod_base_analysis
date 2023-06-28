#!/usr/bin/env python3

import os,json
import sys

directory_name=sys.argv[1]
path_to_json=directory_name

json_files = [pos_json for pos_json in os.listdir(path_to_json)]
countevents=0
for n in range(len(json_files)):
     with open(path_to_json+json_files[n]) as user_files:
             count_dict=json.load(user_files)
             countievents=count_dict['nevents']
     countevents +=countievents
 
print(countevents)

