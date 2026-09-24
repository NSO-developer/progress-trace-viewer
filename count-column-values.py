#!/usr/bin/env python3

import argparse
import ast
from datetime import datetime
from functools import reduce
import re
import sys

import polars as pl


def parseArgs(args):
    parser = argparse.ArgumentParser()
    parser.add_argument('file', type=str,
        help='File to process.')
    parser.add_argument('column', type=str,
        help='Column to analyze.')
    return parser.parse_args(args)


def main(args):
    progress_trace = (pl.scan_csv(args.file)
        .filter(
             # filter out empty rows in case the CSV is not preprocessed
            (pl.col('TIMESTAMP') != '')
        )
    )

    pl.Config().set_tbl_rows(1000)
    pl.Config().set_fmt_str_lengths(100)

    progress_trace = (progress_trace
        .group_by(args.column)
        .agg([
            pl.col(args.column).len().alias('COUNT') 
        ])
    )

    print(progress_trace.collect())




if __name__ == '__main__':
    main(parseArgs(sys.argv[1:]))
