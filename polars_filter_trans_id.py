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
    parser.add_argument('transid', type=int,
            help='Transaction id to filter.')
    parser.add_argument('-o', '--output', type=str,
            help='File to write result to.')
    return parser.parse_args(args)


def main_polars(args):
    progress_trace = pl.scan_csv(args.file).filter(
                            (pl.col('TIMESTAMP') != '') &
                            (pl.col('TRANSACTION ID') == args.transid)
                            )

    result = progress_trace.collect()

    pl.Config().set_tbl_rows(1000)

    if args.output:
        result.write_csv(args.output, separator=',')
    else:
        print(result)

if __name__ == '__main__':
    main_polars(parseArgs(sys.argv[1:]))
