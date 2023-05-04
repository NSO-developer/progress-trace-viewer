#!/usr/bin/env python3

import argparse
from datetime import datetime
import sys
import pandas


def parseArgs(args):
    parser = argparse.ArgumentParser()
    parser.add_argument('file', type=str,
            help='File to process.')
    parser.add_argument('--event', type=str, default='sync-from',
            help='File to process.')
    parser.add_argument('-b', '--begin', type=str,
            help='Start timestamp')
    parser.add_argument('-e', '--end', type=str,
            help='End timestamp')
    return parser.parse_args(args)


def main(args):
    progress_trace = pandas.read_csv(args.file,
                              converters={'TIMESTAMP': datetime.fromisoformat}
                                     )

    have_trace_id = 'TRACE ID' in progress_trace.keys()

    events = progress_trace[
            (progress_trace['DATASTORE'] == 'running') &
             (progress_trace['EVENT TYPE'] == 'stop') &
             (progress_trace['MESSAGE'] == args.event)
    ]
    if args.event:
        events = events[
            events['MESSAGE'] == args.event
        ]
    if args.begin:
        begin = datetime.fromisoformat(args.begin)
        events = events[
            events['TIMESTAMP'] >= begin
        ]
    if args.end:
        end = datetime.fromisoformat(args.end)
        events = events[
            events['TIMESTAMP'] <= end
        ]
    events = events.sort_values('DURATION', ascending=False)

    for i, fields in events.iterrows():
        ts = fields['TIMESTAMP']
        ty = fields['EVENT TYPE']
        dev = fields['DEVICE']
        dur = fields['DURATION']
        tid = fields['TRANSACTION ID']
        msg = fields['MESSAGE']
        ann = fields['ANNOTATION']
        print(f"{ts}  {msg}  {dur:10.1f}  {dev:20} {tid:10} {ann}")


if __name__ == '__main__':
    main(parseArgs(sys.argv[1:]))
