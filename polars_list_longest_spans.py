#!/usr/bin/env python3

import argparse
from datetime import datetime
import sys

import polars as pl
import polars.selectors as cs


def parseArgs(args):
    parser = argparse.ArgumentParser()
    parser.add_argument('file', type=str,
            help='File to process.')
    parser.add_argument('-e', '--event', type=str,
            help='File to process.')
    parser.add_argument('--show-spans', action="store_true", default=False,
            help='Show spans when there is an overlap')
    parser.add_argument('--find-spans', action="store_true", default=False,
            help='Find overlapping spans')
    parser.add_argument('--hide-rows', action="store_true", default=False,
            help='Hide rows')
    return parser.parse_args(args)


def main(args):
    progress_trace = (pl.scan_csv(args.file)
                .filter((pl.col('TIMESTAMP') != '') &
                        (pl.col('DURATION').is_not_null()))
    )
    if args.event:
        progress_trace = progress_trace.filter(pl.col('MESSAGE') == args.event)

    pl.Config().set_tbl_rows(1000)

    data = progress_trace.select([
        'MESSAGE',
        'DURATION',
        'TRANSACTION ID',
        'ATTRIBUTE VALUE'
        ]).sort('DURATION', descending=True).collect()

    print(data)    
if __name__ == '__main__':
    main(parseArgs(sys.argv[1:]))
