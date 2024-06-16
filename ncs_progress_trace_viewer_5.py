#!/usr/bin/env python3

import argparse
import csv
from datetime import datetime
import os
import sys
import time
import traceback

import polars as pl
from rich.live import Live
from rich.bar import Bar
from rich.console import Group
from rich.table import Table
from rich.text import Text
from rich.color import Color


"""
Run with --setup to setup the progress tracing:

progress trace debug
 destination file progress-trace.csv
 destination format csv
 enabled
 verbosity debug
!
"""

def parseArgs(args):
    parser = argparse.ArgumentParser()
    parser.add_argument('-f', '--follow', action='store_true', default=False,
            help="Follow file and graph as traces come.")
    parser.add_argument('-o', action='store_true', default=False,
            help='Graph operational transactions.')
    parser.add_argument('file', type=str, nargs='?',
            help='File to process.')
    parser.add_argument('--setup', action='store_true', default=False,
            help='Config progress trace in NSO.')
    parser.add_argument('--filter', type=str,
            help='Read events to filter from file.')
    parser.add_argument('--tid', type=str,
            help='Filter on transaction id(s).')
    parser.add_argument('--ctid', type=str,
            help='Color transaction id(s).')
    parser.add_argument('-b', '--begin', type=str,
            help='Start timestamp')
    parser.add_argument('-e', '--end', type=str,
            help='End timestamp')
    parser.add_argument('--write', type=str,
            help='Write the progress trace events to file.')
    parser.add_argument('--realtime', action='store_true', default=False,
            help='Skip view updates and sleeps during parsing.')
    parser.add_argument('--speedup', type=int, default=1,
            help='Speedup realtime view n times.')
    parser.add_argument('-t', '--timestamp', action='store_true', default=False,
            help='Show start timestamp.')
    return parser.parse_args(args)


def setup_progress_trace(args):
    try:
        import ncs
        with ncs.maapi.single_write_trans('admin', 'test_context') as t:
            r = ncs.maagic.get_root(t)
            pt = r.progress.trace.create('debug')
            pt.destination.file = 'progress-trace.csv'
            pt.destination.format = 'csv'
            pt.enabled = True
            pt.verbosity = 'debug'
            t.apply()


    except ImportError:
        print("ERROR: Failed to import module ncs.")
        print("Have you sourced ncsrc?")
        sys.exit(1)


def mk_color_numbers():
    return list(filter(lambda i: not i in [4, 16, 17, 18], [i for i in range(1, 232)]))


color_numbers = []
def get_color():
    global color_numbers
    if not color_numbers:
        color_numbers = mk_color_numbers()
    return Color.from_ansi(color_numbers.pop(0))


def follow(thefile):
    '''generator function that yields new lines in a file
    '''
    thefile.seek(0, os.SEEK_END)

    # start infinite loop
    while True:
        line = thefile.readline()
        if not line:
            time.sleep(0.1)
            continue

        yield line


def read_events(filename):
    events = []
    for l in open(filename, 'r'):
        e = l.strip()
        if e and e[0] not in '#-':
            events.append(e)
    return events


def get_table(span_header="Span", rel_time = True):
    table = Table(title="NSO Traces")
    if rel_time:
        table.add_column("Time", max_width=8, justify="right", no_wrap=True)
    else:
        table.add_column("Timestamp", max_width=28, justify="right", no_wrap=True)
    table.add_column("Event", min_width=20, no_wrap=True)
    table.add_column("Context", max_width=20, no_wrap=True)
    table.add_column("Node", max_width=20, no_wrap=True)
    table.add_column("Device", max_width=20, no_wrap=True)
    table.add_column("TId", min_width=3, no_wrap=True)
    table.add_column("Duration", min_width=10, justify="right", no_wrap=True)
    table.add_column(span_header, width=120, no_wrap=True)
    return table


def graph_progress_trace(args, f, events):
    begin = 0.0
    elapsed = 0.0
    last = 0.0

    bars = Group()
    all_spans = []
    open_spans = {}
    tids = {}
    tids_color = {}
    held_locks = {}
    span_duration = Text(" Span 0.0 ms")
    table = get_table(span_header=span_duration, rel_time=not args.timestamp)

    writer = None
    if args.write:
        writer = open(args.write, 'w')
    if args.tid:
        args.tid = args.tid.split(',')

    def new_span(text, key, tid, ts='', ctx='', node='', dev=''):
        if tid not in tids_color:
            if not args.ctid or tid in args.ctid:
                color=get_color()
            else:
                color=Color.from_ansi(237)
            tids_color[tid] = color
        else:
            color=tids_color[tid]
        span = Bar(begin=elapsed, end=elapsed, size=elapsed, color=color)
        d = Text('')
        open_spans[key] = span, d
        all_spans.append(span)
        if tid not in tids:
            tids[tid] = [(text, d, span)]
        else:
            tids[tid].append((text, d, span))
        table.add_row(ts, text, ctx, node, dev, tid, d, span)

    def end_span(key, duration):
        s, d = open_spans[key]
        d.append(f'{duration:0.3f}')
        s.end = elapsed
        if key in open_spans:
            del open_spans[key]


    with Live(table) as live:
        header = None
        n = 1
        ts1 = 0
        try:
            for line in f:
                n += 1
                if writer:
                    writer.write(line)
                l = list(csv.reader([line]))[0]
                if len(l) == 17:
                    have_trace_id = -6
                    have_span_id = 3
                elif len(l) == 18:
                    have_trace_id = -1
                    have_span_id = 0
                elif len(l) == 19:
                    have_trace_id = 0
                    have_span_id = 0
                elif len(l) == 21:
                    have_trace_id = 2
                    have_span_id = 3
                else:
                    print("ERROR: Unsupported number of columns in progress trace"+
                         f"{len(l)}")
                if header is None:
                    if l[0] == 'EVENT TYPE':
                        header = l
                        continue
                    else:
                        header = 'No header.'
                # Skip operational data if not included
                if not args.o and l[5+have_span_id] == 'operational':
                    continue
                # Use events filter if provided
                if events is not None and l[17+have_trace_id] not in events:
                    continue

                tag = l[0]
                ts = datetime.fromisoformat(l[1]).timestamp()
                duration = float(l[2]) if l[2] else 0.0
                sid = l[3+have_span_id]
                tid = l[4+have_span_id]
                ctx = l[6]
                node = l[13+have_trace_id] # Must find a better algo to detect available fields
                dev = l[14+have_trace_id]
                msg = l[17+have_trace_id]
                ann = l[18+have_trace_id]

                key = '-'.join([sid, tid]+l[11+have_trace_id:18+have_trace_id])

                # Filter on transaction id if provided
                if args.tid and tid not in args.tid:
                    continue
                # Filter out message were not interested in (don't know yet what this is)
                if tid == '-1': 
                    continue
                # Filter on time range if provided
                if args.begin and ts < args.begin:
                    continue
                if args.end and ts > args.end:
                    continue

                # Calculate elapsed time
                if begin == 0.0:
                    begin = ts
                    last = ts
                elapsed = ts-begin

                if tag == 'start':
                    if ts1 == 0: ts1=ts
                    sts = str(int(ts-ts1)) if not args.timestamp else l[1]
                    new_span(msg, key, tid, ts=sts, ctx=ctx, node=node, dev=dev)
                elif tag == 'stop':
                    if key in open_spans:
                        end_span(key, duration)

                # Update span sizes
                for s in all_spans:
                    s.size = elapsed
                for s,_ in open_spans.values():
                    s.end = elapsed

                # Update span duration column
                span_duration.plain = f'Span {elapsed:0.3f} s'
                if (args.follow or last-ts>0.1) and args.realtime:
                    live.refresh()

                # Sleep if not following and in realtime mode
                if not args.follow and args.realtime and last:
                    delay = ts-last
                    if delay > 0:
                        time.sleep(delay/args.speedup)
                last = ts
            live.refresh()
        except Exception as e:
            print(f"ERROR: At line {n}")
            print(e)
            print(traceback.format_exc())



def main(args):
    if args.setup:
        setup_progress_trace(args)
        sys.exit(0)
    if args.file is None:
        print("You must specify a file to process.")
        sys.exit(1)
    if args.begin is not None:
        args.begin = datetime.fromisoformat(args.begin).timestamp()
    if args.end is not None:
        args.end = datetime.fromisoformat(args.end).timestamp()
    events = None
    if args.filter is not None:
        events = read_events(args.filter)
    f = open(args.file, 'r')
    if args.follow:
        f = follow(f)
    try:
        graph_progress_trace(args, f, events)
    except KeyboardInterrupt:
        print()
        print("Stopped")


if __name__ == '__main__':
    main(parseArgs(sys.argv[1:]))
