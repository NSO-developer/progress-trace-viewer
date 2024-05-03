#!/usr/bin/env python3
#-*- coding: utf-8; mode: python; py-indent-offset: 4; tab-width: 4 -*-

import argparse
import sys
import polars as pl


def parseArgs(args=None):
    parser = argparse.ArgumentParser()
    parser.add_argument('files', type=str, nargs='+',
            help='File(s) to process.')
    parser.add_argument('-o', '--output', type=str,
            help='File to write result to.')
    parser.add_argument('--oper', action='store_true',
            help='Include operational messages.')
    parser.add_argument('--info', action='store_true',
            help='Include info messages.')
    return parser.parse_args(args)

# ======== MAIN ==========

def main(args):
    pt = []
    for f in args.files:
        n, f = f.split(':') if ':' in f else (f, f)
        pt.append(pl.scan_csv(f).sort('TIMESTAMP').with_columns(pl.lit(n).alias('NODE')))

    output = pt.pop(0)
    for p in pt:
        output = output.merge_sorted(p, key="TIMESTAMP")   

    if not args.oper:
        output = output.filter(pl.col('DATASTORE') != 'operational')       

    if not args.info:
        output = output.filter(pl.col('EVENT TYPE') != 'info')
    output = output.with_columns(pl.col("MESSAGE").str.replace_all("\n", "/"))

    if args.output:
        output.collect().write_csv(args.output, separator=',')
    else:
        pl.Config().set_tbl_rows(1000)
        print(output.collect())


if __name__ == '__main__':
    main(parseArgs())