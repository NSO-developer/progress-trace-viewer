#!/usr/bin/env python3

import argparse
from datetime import datetime
import sys

import polars as pl


def parseArgs(args):
    parser = argparse.ArgumentParser()
    parser.add_argument('file', type=str,
            help='File to process.')
    parser.add_argument('event', type=str,
            help='File to process.')
    parser.add_argument('--show-spans', action="store_true", default=False,
            help='Show spans when there is an overlap')
    parser.add_argument('--show-tid', action="store_true", default=False,
            help='Show spans when there is an overlap')
    parser.add_argument('--find-spans', action="store_true", default=False,
            help='Find overlapping spans')
    parser.add_argument('--hide-rows', action="store_true", default=False,
            help='Hide rows')
    return parser.parse_args(args)



def process_overlap(args, progress_trace):
    spans = {}

    for fields in progress_trace.collect().iter_rows(named=True):
        et = fields['EVENT TYPE']
        m = fields['MESSAGE']
        ts = fields['TIMESTAMP']
        tid = fields['TRANSACTION ID']
        if et == 'start':
            #if tid not in spans: continue # ignore start events that are not in progress
            spans[tid] = 1
            if not args.hide_rows:
                print(f"{ts}  {len(spans)} {m}  {' '.join(map(str, spans.keys()))}")
            if args.show_spans and len(spans.keys())>1:
                print(spans)
        elif et == 'stop':
            spans.pop(tid, None) # No exception if tid not found
            if not args.hide_rows:
                print(f"{ts}  {len(spans)} {m}  {' '.join(map(str, spans.keys()))}")
        p_ts = ts
        p_tid = tid


def main(args):
    progress_trace = pl.scan_csv(args.file).filter(
                            (pl.col('TIMESTAMP') != '') &
                            (pl.col('DATASTORE') == 'running') &
                            (pl.col('MESSAGE') == args.event)
    ).sort('TIMESTAMP')
    if args.find_spans:
        args.hide_rows = True
        args.show_spans = True
    process_overlap(args, progress_trace)


if __name__ == '__main__':
    main(parseArgs(sys.argv[1:]))
