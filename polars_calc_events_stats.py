#!/usr/bin/env python3

import argparse
from datetime import datetime
import sys

import polars as pl


def parseArgs(args):
    parser = argparse.ArgumentParser()
    parser.add_argument('file', type=str,
            help='File to process.')
    return parser.parse_args(args)


def get_statistics(progress_trace, datastore='running'):
    d = progress_trace.filter((pl.col('DATASTORE') == datastore) &
                (pl.col('EVENT TYPE') == 'stop') &
                ~(pl.col('MESSAGE').str.starts_with('check conflict'))
    )


    duration_grouped_by_message = d.group_by('MESSAGE').agg([
                    pl.col('MESSAGE').len().alias('COUNT'),
                    pl.col('DURATION').sum().alias('SUM'),
                    pl.col('DURATION').std().alias('STD'),
                    pl.col('DURATION').mean().alias('MEAN'),
                    pl.col('DURATION').min().alias('MIN'),
                    pl.col('DURATION').max().alias('MAX')
                    ])

    return duration_grouped_by_message.collect().sort('MESSAGE')


def main(args):
    progress_trace = pl.scan_csv(args.file).filter(
                            (pl.col('TIMESTAMP') != '')
    )

    pl.Config().set_tbl_rows(1000)

    print("=== RUNNING ===")
    print(get_statistics(progress_trace))
#    print("\n=== NO DATASTORE ===")
#    print(get_statistics(progress_trace, ''))
#    print("\n=== OPERATIONAL ===")
#    print(get_statistics(progress_trace, 'operational'))

if __name__ == '__main__':
    main(parseArgs(sys.argv[1:]))
