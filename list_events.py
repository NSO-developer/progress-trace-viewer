#!/usr/bin/env python3

import argparse
from datetime import datetime
import sys

import pandas


def parseArgs(args):
    parser = argparse.ArgumentParser()
    parser.add_argument('file', type=str,
            help='File to process.')
    parser.add_argument('-n', '--negate', action='store_true', default=False,
            help='Negate/comment events.')
    return parser.parse_args(args)


def main(args):
    prefix = '-' if args.negate else ''

    progress_trace = pandas.read_csv(args.file, index_col='TIMESTAMP',
                                     low_memory=False)

    d = progress_trace[(progress_trace['DATASTORE'] == 'running') &
                (progress_trace['EVENT TYPE'] == 'stop')]

    duration_grouped_by_message = d.groupby('MESSAGE')['MESSAGE']
    for name, ev in duration_grouped_by_message:
        print(f'{prefix}{name}')


if __name__ == '__main__':
    main(parseArgs(sys.argv[1:]))
