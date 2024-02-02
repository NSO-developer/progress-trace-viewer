#!/usr/bin/env python3

import argparse
from datetime import datetime
import sys
import time

import polars as pl

def parseArgs(args):
    parser = argparse.ArgumentParser()
    parser.add_argument('file', type=str,
            help='File to process.')
    parser.add_argument('-n', '--negate', action='store_true', default=False,
            help='Negate/comment events.')
    return parser.parse_args(args)


def main_polars(args):
    prefix = '-' if args.negate else ''

    progress_trace = pl.scan_csv(args.file).filter(
                            (pl.col('TIMESTAMP') != '') &
                            (pl.col('DATASTORE') == 'running') &
                            (pl.col('EVENT TYPE') ==
                             'stop')).group_by('MESSAGE').len()

    for message, count in progress_trace.collect().iter_rows():
       print(f'{message: <50} {count: >7}')

if __name__ == '__main__':
    main_polars(parseArgs(sys.argv[1:]))
