#!/usr/bin/env python3

import argparse
import csv
from datetime import datetime, timedelta
import os
import sys
import time

def parseArgs(args=None):
    parser = argparse.ArgumentParser()
    parser.add_argument('file', type=str,
            help='File to process.')
    parser.add_argument('-o', '--output', type=str,
            help='Write result to.')
    return parser.parse_args(args)

def main(args):
    f = open(args.file, 'r')
    if args.output:
        fieldnames = ['EVENT TYPE', 'TIMESTAMP', 'DURATION', 'SESSION ID', 'TRANSACTION ID', 'DATASTORE', 'CONTEXT', 'TRACE ID', 'SUBSYSTEM', 'PHASE', 'SERVICE', 'SERVICE PHASE', 'COMMIT QUEUE ID', 'NODE', 'DEVICE', 'DEVICE PHASE', 'PACKAGE', 'MESSAGE', 'ANNOTATION']
        writer = csv.DictWriter(open(args.output, 'w'), fieldnames=fieldnames)
        writer.writeheader()

    held_locks = {}

    td = timedelta(seconds=0.000001)

    for l in csv.DictReader(f):
        e = None
        tag = l['EVENT TYPE']
        ts = datetime.fromisoformat(l['TIMESTAMP'])# .timestamp()
        duration = float(l['DURATION']) if l['DURATION'] else 0.0
        tid = l['TRANSACTION ID']
        msg = l['MESSAGE']
        ann = l['ANNOTATION']

        if msg == 'grabbing transaction lock':
            l['MESSAGE'] = msg = 'taking transaction lock'

        if tag == 'stop' and msg == 'taking transaction lock':
            held_locks[tid] = ts
            e = l.copy()
            e.update({
                'EVENT TYPE': 'start',
                'MESSAGE': 'holding transaction lock',
                'TIMESTAMP': (ts+td).isoformat(), # Make injected events appear after the original
                'DURATION': ''
            })
        elif (tag == 'stop' and msg == 'applying transaction' and ann == 'stopped'):
            if tid in held_locks:
                sts = held_locks.pop(tid)
                e = l.copy()
                e.update({
                    'EVENT TYPE': 'stop',
                    'MESSAGE': 'holding transaction lock',
                    'TIMESTAMP': (ts-td).isoformat(), # Make injected events appear before the original
                    'DURATION': (ts-td-sts).total_seconds()
                })
            else:
                print("Failed to find start event for transaction lock", tid, file=sys.stderr)
        elif (tag == 'info' and msg == 'releasing transaction lock'):
            if tid in held_locks:
                sts = held_locks.pop(tid)
                e = l.copy()
                e.update({
                    'EVENT TYPE': 'stop',
                    'MESSAGE': 'holding transaction lock',
                    'TIMESTAMP': ts.isoformat(),
                    'DURATION': (ts-sts).total_seconds()
                })
                l = None # Prevent original event from being printed
            else:
                print("Failed to find start event for transaction lock", tid, file=sys.stderr)

        if args.output:
            if l:
                l['MESSAGE'] = f'{l["MESSAGE"]}'
                writer.writerow(l)
            if e:
                writer.writerow(e)
        else:
            if l:
                print(','.join(l.values()))
            if e:
                print(','.join(map(str, e.values())))
    
    for tid, sts in held_locks.items():
        print("Failed to find stop event for transaction lock", tid, file=sys.stderr)


if __name__ == '__main__':
    main(parseArgs())
