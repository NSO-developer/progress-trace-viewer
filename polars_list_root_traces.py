#!/usr/bin/env python3
#-*- coding: utf-8; mode: python; py-indent-offset: 4; tab-width: 4 -*-

import argparse
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
    return parser.parse_args(args)


def main(args):
    pl.Config().set_tbl_rows(40)

    root_spans = [
        pl.col('TIMESTAMP') != '', # Filter out empty timestamps for non pre-processed trace files.
        pl.col('EVENT TYPE') == 'stop',
        pl.col('PARENT SPAN ID').is_null() |      # Per definition a root span
        (pl.col('MESSAGE') == 'restconf edit'),   # restconf edit has parent span set when it shouldn't
    ]
    filter = []
    if args.event:
        filter.append(pl.col('MESSAGE') == args.event)

    progress_trace = (pl.scan_csv(args.file)
                .filter(root_spans)
    )
    if filter:
        progress_trace = progress_trace.filter(filter)

    data = progress_trace.select([
        'MESSAGE',
        'DURATION',
        'SPAN ID',
        'TRANSACTION ID',
        'CONTEXT',
        ]).sort('DURATION',
                                 descending=True).collect()

    print(data)


if __name__ == '__main__':
    main(parseArgs(sys.argv[1:]))
