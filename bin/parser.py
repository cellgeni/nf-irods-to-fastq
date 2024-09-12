#!/usr/bin/env python

import re
import sys
import json

def main():
    # read positional argument with file path
    filepath = sys.argv[1]

    # read file with cram metadata
    with open(filepath, 'r') as file:
        pathline = file.readline() # first line include the path of cram file
        metadata_raw = file.read()

    # Here match the following pattern in metadata:
    ### attribute: ([\w:]+)
    ### value: ([\w\d\S ^b]+)

    pattern = re.compile(r'attribute: ([\w:]+)\nvalue: ([\w\d\S ^b]+)')
    meta_parsed = dict(pattern.findall(metadata_raw))

    # Create json with selected meta
    meta_processed = {
        'sample': meta_parsed['sample'],
        'cram_path': pathline.strip(':\n').split(' ')[-1],
        'fastq_name': f"{meta_parsed['sample']}_S{meta_parsed['tag_index']}_L{str(meta_parsed.get('lane', 1)).zfill(3)}",
        'sample_supplier_name': meta_parsed.get('sample_supplier_name', 'NaN'),
        'library_type': meta_parsed.get('library_type', 'NaN'),
        'total_reads_irods': meta_parsed.get('total_reads', 'NaN')
    }

    # Dump processed meta to json
    with open(f"meta.json", 'w') as json_file:
        json.dump(meta_processed, json_file)


if __name__ == '__main__':
    main()
