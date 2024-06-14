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
    parser.add_argument('--trid', type=str,
            help='Trace id(s) to filter.')
    parser.add_argument('--et', type=str,
            help='Event type(s) to filter.')
    parser.add_argument('--msg', type=str,
            help='Message(s) to filter.')
    parser.add_argument('--ctx', type=str,
            help='Context(s) to filter.')
    parser.add_argument('--ds', type=str,
            help='Datastore to filter.')
    parser.add_argument('--device', type=str,
            help='Device(s) to filter.')
    parser.add_argument('--node', type=str,
            help='Node(s) to filter.')
    parser.add_argument('--ann', type=str,
            help='Annotation(s) to filter.')
    parser.add_argument('--begin', type=str,
            help='Beginning timestamp.')
    parser.add_argument('--end', type=str,
            help='Ending timestamp.')
    parser.add_argument('-m', '--mincols', action='store_true',
            help='Less number of columns.')
    parser.add_argument('-n', '--nodyncols', action='store_true',
            help='Remove dynamic columns (Useful for comparing files for different runs).')
    parser.add_argument('-o', '--output', type=str,
            help='File to write result to.')
    parser.add_argument('-s', '--start', action='store_true',
            help='Calculate start timestamp from timestamp end duration.')
    return parser.parse_args(args)


def main_polars(args):
    progress_trace = pl.scan_csv(args.file).\
        with_columns(pl.col('TIMESTAMP').str.to_datetime("%Y-%m-%dT%H:%M:%S%.f")).\
        sort('TIMESTAMP')
    if args.tid:
        progress_trace = progress_trace.filter(
            (pl.col('TRANSACTION ID').is_in(list(map(int, args.tid.split(',')))))
        )
    if args.trid:
        progress_trace = progress_trace.filter(
            (pl.col('TRACE ID').is_in(list(args.trid.split(','))))
        )
    if args.et:
        progress_trace = progress_trace.filter(
            (pl.col('EVENT TYPE').is_in(list(args.et.split(','))))
        )
    if args.msg:    
        progress_trace = progress_trace.filter(
            (pl.col('MESSAGE').is_in(list(args.msg.split(','))))
        )
    if args.ctx:
        progress_trace = progress_trace.filter(
            (pl.col('CONTEXT').is_in(list(args.ctx.split(','))))
        )
    if args.ds:
        progress_trace = progress_trace.filter(
            (pl.col('DATASTORE') == args.ds)
        )
    if args.device:    
        progress_trace = progress_trace.filter(
            (pl.col('DEVICE').is_in(list(args.device.split(','))))
        )
    if args.node:    
        progress_trace = progress_trace.filter(
            (pl.col('NODE').is_in(list(args.node.split(','))))
        )
    if args.ann:
        progress_trace = progress_trace.filter(
            (pl.col('ANNOTATION').is_in(list(args.ann.split(','))))
        )
    if args.begin:
        progress_trace = progress_trace.filter(
            (pl.col('TIMESTAMP') >= datetime.fromisoformat(args.begin))
         )
    if args.end:
        progress_trace = progress_trace.filter(
            (pl.col('TIMESTAMP') <= datetime.fromisoformat(args.end))
         )
    if args.start:
        progress_trace = progress_trace.with_columns(
            (pl.col('TIMESTAMP')-(pl.col('DURATION')*1000000).cast(pl.Duration('us'))).alias('START')
        )
    result = progress_trace.collect()
    columns = progress_trace.columns

    if args.mincols:
        columns = [
            'EVENT TYPE',
            'MESSAGE',
            'ANNOTATION',
            'CONTEXT',
            'TIMESTAMP',
            'DURATION',
            'TRANSACTION ID',
            'TRACE ID', 
            'NODE', 
            'DEVICE',
        ]
        if args.start:
            columns.append('START')

    if args.nodyncols:
        columns = [
            'EVENT TYPE',
            'MESSAGE',
            'ANNOTATION',
            'CONTEXT',
            'NODE', 
            'DEVICE', 
        ]

    result = result.select(columns)
    pl.Config().set_tbl_rows(1000)

    if args.output:
        result.write_csv(args.output, separator=',')
    else:
        with pl.Config(tbl_cols=-1, fmt_str_lengths=100):
            print(result)

if __name__ == '__main__':
    main_polars(parseArgs(sys.argv[1:]))
