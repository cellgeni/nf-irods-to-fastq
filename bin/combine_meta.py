#!/usr/bin/env python

import os
import sys
import json
import csv

# read positional argument with filedir path
dirpath = sys.argv[1].strip('/')

# read all json files to meta_list
meta_list = list()
for filename in os.listdir(dirpath):
    with open(f'{dirpath}/{filename}', 'r') as file:
        sample_meta = json.load(file)
        meta_list.append(sample_meta)

# save the field names
fieldnames = sample_meta.keys()

# sort the the data by sample name 
meta_list = sorted(meta_list, key=lambda x: x['sample'])

# write all metadata to csv
with open('metadata.csv', mode='w') as csv_file:
    # create writer object
    writer = csv.DictWriter(csv_file, fieldnames=fieldnames)

    # write the data
    writer.writeheader()
    for sample_meta in meta_list:
        writer.writerow(sample_meta)
