#!/usr/bin/env python3

import argparse
from datetime import datetime
import sys

import polars as pl

# TODO: Useful features
# - Report incomplete spans at start (and end?) Costly to include in the count
# - Option to include incomplete spans in the count

def parseArgs(args):
    parser = argparse.ArgumentParser()
    parser.add_argument('file', type=str,
            help='File to process.')
    parser.add_argument('--event', type=str,
            help='File to process.')
    parser.add_argument('--show-spans', action="store_true", default=False,
            help='Show spans when there is an overlap')
    parser.add_argument('--show-tid', action="store_true", default=False,
            help='Show spans when there is an overlap')
    parser.add_argument('--find-spans', action="store_true", default=False,
            help='Find overlapping spans')
    parser.add_argument('--hide-rows', action="store_true", default=False,
            help='Hide rows')
    parser.add_argument('--debug', action="store_true", default=False,
            help='Show debug information')
    return parser.parse_args(args)

class Span:
    __slots__ = ['start', 'max']
    def __init__(self, count):
        self.start = count
        self.max = count

def process_overlap(args, progress_trace):
    spans = {}
    incomplete_spans = []
    count_open_spans = 0

    try:
        for fields in progress_trace.collect().iter_rows(named=True):
            et = fields['EVENT TYPE']
            d = fields['DURATION']
            m = fields['MESSAGE']
            ts = fields['TIMESTAMP']
            tid = fields['TRANSACTION ID']
            trid = fields['TRACE ID']
            key = (trid, m)
            if et == 'start':
                for span in spans.values():
                    span.max += 1
                spans[key] = Span(count_open_spans)
                if args.debug and not args.hide_rows:
                    print(f"{ts}  {trid}  {et:7}{m:40} {count_open_spans}", end='')
                if args.debug and args.show_spans and len(spans.keys())>1:
                    print(f"  {' '.join(map(str, spans.keys()))}", end='')
                if args.debug: print()
                count_open_spans += 1
            elif et == 'stop':
                count_open_spans -= 1
                span = spans.pop(key, None)
                if span is not None:
                    if not args.hide_rows:
                        print(f"{ts}  {d:>10.3f}  {trid}  {et:7}{m:40} {span.start} {span.max} {count_open_spans}", end='')   
                    if args.show_spans and len(spans.keys())>1:
                        print(f"  {' '.join(map(str, spans.keys()))}")
                    print()
                else:
                    incomplete_spans.append((ts, tid, trid, m))
            p_ts = ts
            p_tid = tid
    except KeyboardInterrupt:
        print()
        print("Stopped")
        print()

    if incomplete_spans:
        print(f"Incomplete spans {len(incomplete_spans)}:")
        for ts, tid, trid, m in incomplete_spans:
            print(f"{ts}  {tid}  {trid:<10}  {m:40}")


def main(args):
    progress_trace = pl.scan_csv(args.file).filter(
                            (pl.col('TIMESTAMP') != '') &
                            (pl.col('DATASTORE') == 'running')
    )
    if args.event:
        progress_trace = progress_trace.filter(pl.col('MESSAGE') == args.event)
    else:
        # Get root spans
        progress_trace = progress_trace.filter((pl.col('PARENT SPAN ID').is_null()) |
        (pl.col('MESSAGE') == 'restconf edit'))
    progress_trace = progress_trace.sort('TIMESTAMP')
    if args.find_spans:
        args.hide_rows = True
        args.show_spans = True
    process_overlap(args, progress_trace)


if __name__ == '__main__':
    main(parseArgs(sys.argv[1:]))
