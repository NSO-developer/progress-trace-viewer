#!/usr/bin/env python3
#-*- coding: utf-8; mode: python; py-indent-offset: 4; tab-width: 4 -*-

import argparse
from datetime import datetime
import sys
import time

import polars as pl

def parseArgs(args):
    parser = argparse.ArgumentParser()
    parser.add_argument('file', type=str,
            help='File to process.')
    parser.add_argument('--tid', type=str,
            help='Transaction id(s) to filter.')
    parser.add_argument('--msg', type=str,
            help='Message(s) to filter.')
    parser.add_argument('--device', type=str,
            help='Device(s) to filter.')
    parser.add_argument('--node', type=str,
            help='Node(s) to filter.')
    parser.add_argument('begin', type=str,
            help='Beginning timestamp.')
    parser.add_argument('end', type=str,
            help='Ending timestamp.')
    parser.add_argument('-o', '--output', type=str,
            help='File to write result to.')
    return parser.parse_args(args)


def main_polars(args):
    progress_trace = pl.scan_csv(args.file).with_columns(pl.col('TIMESTAMP').str.to_datetime("%Y-%m-%dT%H:%M:%S%.f"))
    if args.tid:
        progress_trace = progress_trace.filter(
            (pl.col('TRANSACTION ID').is_in(list(map(int, args.tid.split(',')))))
        )
    if args.msg:    
        progress_trace = progress_trace.filter(
            (pl.col('MESSAGE').is_in(list(args.tid.split(','))))
        )
    if args.device:    
        progress_trace = progress_trace.filter(
            (pl.col('DEVICE').is_in(list(args.device.split(','))))
        )
    if args.node:    
        progress_trace = progress_trace.filter(
            (pl.col('NODE').is_in(list(args.node.split(','))))
        )
    if args.begin:
        progress_trace = progress_trace.filter(
            (pl.col('TIMESTAMP') >= datetime.fromisoformat(args.begin))
         )
    if args.end:
        progress_trace = progress_trace.filter(
            (pl.col('TIMESTAMP') <= datetime.fromisoformat(args.end))
         )

    result = progress_trace.collect()

    pl.Config().set_tbl_rows(1000)

    if args.output:
        result.write_csv(args.output, separator=',')
    else:
        print(result)

if __name__ == '__main__':
    main_polars(parseArgs(sys.argv[1:]))
