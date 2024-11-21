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
    parser.add_argument('--find-spans', action="store_true", default=False,
            help='Find overlapping spans')
    parser.add_argument('--hide-rows', action="store_true", default=False,
            help='Hide rows')
    return parser.parse_args(args)


def main(pt, hide_rows, show_spans, find_spans, event):
    progress_trace = pt.filter(
                        (pl.col('TIMESTAMP') != '') &
                        (pl.col('DATASTORE') == 'running') &
                        (pl.col('MESSAGE') == event)
    )
    if find_spans:
        hide_rows = True
        show_spans = True
        
    spans = {}

    for fields in progress_trace.collect().iter_rows(named=True):
        et = fields['EVENT TYPE']
        m = fields['MESSAGE']
        ts = fields['TIMESTAMP']
        tid = fields['TRANSACTION ID']
        if et == 'start':
            #if tid not in spans: continue # ignore start events that are not in progress
            spans[tid] = 1
            if not hide_rows:
                print(f"{ts}  {len(spans)} {m}")
            if show_spans and len(spans.keys())>1:
                print(spans)
        elif et == 'stop':
            spans.pop(tid, None) # No exception if tid not found
            if not hide_rows:
                print(f"{ts}  {len(spans)} {m}")
        

if __name__ == '__main__':
    args = parseArgs(sys.argv[1:])
    progress_trace = pl.scan_csv(args.file)
    main(progress_trace, args.hide_rows, args.show_spans, args.find_spans, args.event)
