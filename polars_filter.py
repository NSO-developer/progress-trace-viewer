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
    parser.add_argument('text', type=str,
            help='Message to filter.')
    parser.add_argument('-o', '--output', type=str,
            help='File to write result to.')
    parser.add_argument('-c', '--column', type=str, default='MESSAGE',
            help='Column to filter on.')
    return parser.parse_args(args)


def main_polars(args):
    args.text = list(map(int, args.text.split(',')))
    progress_trace = pl.scan_csv(args.file).filter(
                            (pl.col(args.column).is_in(args.text))# &
                            #(pl.col('DEVICE') == 'RFS1')
                            )
    
    result = progress_trace.collect()

    pl.Config().set_tbl_rows(1000)

    if args.output:
        result.write_csv(args.output, separator=',')
    else:
        print(result)

if __name__ == '__main__':
    main_polars(parseArgs(sys.argv[1:]))
