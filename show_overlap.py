#!/usr/bin/env python3

import argparse
from datetime import datetime
import sys
import pandas


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



def process_overlap(args, progress_trace):
    spans = {}

    filtered_events = progress_trace[progress_trace['MESSAGE'] == args.event]
    for ts, fields in filtered_events.sort_index().iterrows():
        et = fields['EVENT TYPE']
        tid = fields['TRANSACTION ID']
        if et == 'start':
            assert(tid not in spans)
            spans[tid] = 1
            if not args.hide_rows:
                print(f"{ts}  {len(spans)}")
            if args.show_spans and len(spans.keys())>1:
                print(spans)
        elif et == 'stop':
            del spans[tid]
            if not args.hide_rows:
                print(f"{ts}  {len(spans)}")
        p_ts = ts
        p_tid = tid


def main(args):
    progress_trace = pandas.read_csv(args.file,
                              index_col='TIMESTAMP',
                              converters={'TIMESTAMP': datetime.fromisoformat}
                                     ).sort_values('TIMESTAMP')
    running = progress_trace[progress_trace['DATASTORE'] == 'running']
    if args.find_spans:
        args.hide_rows = True
        args.show_spans = True
    process_overlap(args, running)


if __name__ == '__main__':
    main(parseArgs(sys.argv[1:]))
