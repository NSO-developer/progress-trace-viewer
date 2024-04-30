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



def main(args):
    progress_trace = pl.scan_csv(args.file)

    pl.Config().set_tbl_rows(1000)

    grouped_by_message = progress_trace.group_by('ATTRIBUTE NAME').agg([
                    pl.col('ATTRIBUTE NAME').len().alias('COUNT'),
                    ])

    result = grouped_by_message.collect().sort('ATTRIBUTE NAME')

    print(result.select(['ATTRIBUTE NAME', 'COUNT']))

if __name__ == '__main__':
    main(parseArgs(sys.argv[1:]))
