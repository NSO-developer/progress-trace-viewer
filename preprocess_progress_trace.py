#!/usr/bin/env python3

import argparse
import csv
from datetime import datetime
import os
import sys
import time

"""
Run with --setup to setup the progress tracing:

progress trace debug
 destination file progress-trace.csv
 destination format csv
 enabled
 verbosity debug
!
"""

def parseArgs(args):
    parser = argparse.ArgumentParser()
    parser.add_argument('input', type=str,
            help='File to process.')
    parser.add_argument('output', type=str,
            help='File to write.')
    return parser.parse_args(args)


# Function to extract the comma separated names after the pattern "fieldnames:"
# from a string with the format. The single quotes are removed:
# "dict contains fields not in fieldnames: 'ATTRIBUTE VALUE', 'ATTRIBUTE NAME'"
def extract_fieldnames(line):
    if 'fieldnames:' in line:
        return [x.strip(" '") for x in line.split(':')[1].split(',')]
    return None


ADDITIONAL_FIELDNAMES = [
    'ACTION',
    'DEVICE',
    'DEVICE_PHASE',
    'SERVICE',
    'SERVICE_OPERATION',
    'SERVICE_PHASE',
    'TEMPLATE',
    'TRANSACTION_PHASE'
]

def write_row(writer, row, missing_fieldnames):
    try:
        writer.writerow(row)
    except ValueError as e:
        for n in extract_fieldnames(e.args[0]):
            missing_fieldnames.add(n)
            del row[n]
        writer.writerow(row)

def main(args):
    missing_fieldnames = set()
    try:
        reader = csv.DictReader(open(args.input, 'r'))
        fieldnames = list(reader.fieldnames)
        fieldnames.remove('ATTRIBUTE NAME')
        fieldnames.remove('ATTRIBUTE VALUE')
        fieldnames += ADDITIONAL_FIELDNAMES
        writer = csv.DictWriter(open(args.output, 'w'), fieldnames =
                                fieldnames, extrasaction='raise')
        set_fieldnames = set(fieldnames) 
        writer.writeheader()
        last_row = None
        for row in reader:
            if row['TIMESTAMP'] != "":
                if last_row is not None:
                    del last_row['ATTRIBUTE NAME']
                    del last_row['ATTRIBUTE VALUE']
                    nf = set(last_row.keys()) - set_fieldnames
                    if nf:
                        for n in nf:
                            del last_row[n]
                    missing_fieldnames.update(nf)
                    writer.writerow(last_row)
                last_row = row
            an = row['ATTRIBUTE NAME']
            if an != "":
                last_row[an.upper()] = row['ATTRIBUTE VALUE']
        if last_row:
            writer.writerow(last_row)
    except KeyboardInterrupt:
        print()
        print("Stopped")
    if missing_fieldnames:
        print("Missing fieldnames:")
        for n in missing_fieldnames:
            print(n)

if __name__ == '__main__':
    main(parseArgs(sys.argv[1:]))
