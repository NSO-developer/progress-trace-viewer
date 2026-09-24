#!/usr/bin/env python3

import argparse
import sys

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


def main_polars(pt, trans_id):
    progress_trace = pt.filter(
                        (pl.col('TIMESTAMP') != '') &
                        (pl.col('TRANSACTION ID') == trans_id)
                        )
    

    return progress_trace.collect()

if __name__ == '__main__':
    args = parseArgs(sys.argv[1:])
    progress_trace = pl.scan_csv(args.file)
    result = main_polars(progress_trace, args.transid)
        
    pl.Config().set_tbl_rows(1000)

    if args.output:
        result.write_csv(args.output, separator=',')
    else:
        with pl.Config(tbl_cols=-1):
            print(result)
    
