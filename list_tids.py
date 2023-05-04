#!/usr/bin/env python3

import argparse
from datetime import datetime
import sys
import pandas


def parseArgs(args):
    parser = argparse.ArgumentParser()
    parser.add_argument('file', type=str,
            help='File to process.')
    parser.add_argument('--event', type=str,
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

    sync_from_events = progress_trace[
            (progress_trace['DATASTORE'] == 'running') &
            ((progress_trace['EVENT TYPE'] == 'start') |
             (progress_trace['EVENT TYPE'] == 'stop'))
    ]
    if args.event:
        sync_from_events = sync_from_events[
            sync_from_events['MESSAGE'] == args.event
        ]
    if args.begin:
        begin = datetime.fromisoformat(args.begin)
        sync_from_events = sync_from_events[
            sync_from_events['TIMESTAMP'] >= begin
        ]
    if args.end:
        end = datetime.fromisoformat(args.end)
        sync_from_events = sync_from_events[
            sync_from_events['TIMESTAMP'] <= end
        ]

    tids = []
    tids_msgs = []
    for name, fields in sync_from_events.iterrows():
        evt = fields['EVENT TYPE']
        tid = fields['TRANSACTION ID']
        msg = fields['MESSAGE']
        ann = fields['ANNOTATION']
        if evt == 'start':
            if tid not in tids:
                tids.append(tid)
                tids_msgs.append((tid, msg))
        elif evt == 'stop':
            if (tid, msg) in tids_msgs:
                tids_msgs.remove((tid, msg))
                tids.remove(tid)
                print(f"{name}  {msg:15} {tid} {ann}")


if __name__ == '__main__':
    main(parseArgs(sys.argv[1:]))
