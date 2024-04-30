#!/usr/bin/env python3

import argparse
import csv
from datetime import datetime
import os
import sys
import time

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
            help='Filter on transaction id.')
    parser.add_argument('--write', type=str,
            help='Write the progress trace events to file.')
    parser.add_argument('--realtime', action='store_true', default=False,
            help='Skip view updates and sleeps during parsing.')
    parser.add_argument('--speedup', type=int, default=1,
            help='Speedup realtime view n times.')
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


def get_table(tids = None, span_header="Span"):
    table = Table(title="NSO Traces")
    table.add_column("Time", max_width=8, justify="right", no_wrap=True)
    table.add_column("Event", min_width=20, no_wrap=True)
    table.add_column("Context", max_width=20, no_wrap=True)
    table.add_column("Device", max_width=20, no_wrap=True)
    table.add_column("TId", min_width=3, no_wrap=True)
    table.add_column("Duration", min_width=10, justify="right", no_wrap=True)
    table.add_column(span_header, width=120, no_wrap=True)
    if tids is not None:
        for tid, spans in tids.items():
            for text, d, span in spans:
                table.add_row(f'{text} {tid}', d, span)
    return table


def graph_progress_trace(args, f, events):
    begin = 0.0
    size = 0.0
    last = 0.0

    bars = Group()
    spans = {}
    tids = {}
    tids_color = {}
    spans_running = {}
    held_locks = {}
    span_duration = Text(" Span 0.0 ms")
    table = get_table(span_header=span_duration)

    writer = None
    if args.write:
        writer = open(args.write, 'w')
    if args.tid:
        if ',' in args.tid:
            args.tid = args.tid.split(',')
        else:
            args.tid = [ args.tid ]

    def new_span(text, key, tid, ts='', ctx='', dev=''):
        if tid not in tids_color:
            color=get_color()
            tids_color[tid] = color
        else:
            color=tids_color[tid]
        span = Bar(begin=size, end=size, size=size, color=color)
        spans_running[key] = span
        d = Text('')
        spans[key] = span, d
        if tid not in tids:
            tids[tid] = [(text, d, span)]
        else:
            tids[tid].append((text, d, span))
        table.add_row(ts, text, ctx, dev, tid, d, span)

    def end_span(key, duration):
        s, d = spans[key]
        
        d.append(f'{duration:0.3f}')
        s.end = size
        if key in spans_running:
            del spans_running[key]

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
                if not args.o and l[5+have_span_id] == 'operational':
                    continue
                if events is not None and l[17+have_trace_id] not in events:
                    continue
                tag = l[0]
                ts = datetime.fromisoformat(l[1]).timestamp()
                duration = float(l[2]) if l[2] else 0.0
                sid = l[3+have_span_id]
                tid = l[4+have_span_id]
                key = '-'.join([sid, tid]+l[11+have_trace_id:18+have_trace_id])
                text = l[17+have_trace_id]
                ctx = l[6]
                dev = l[14+have_trace_id]

                if args.tid and tid not in args.tid:
                    continue

                if begin == 0.0:
                    begin = ts
                    last = ts
                size = ts-begin
                if tag == 'start':
                    if ts1 == 0: ts1=ts
                    new_span(text, key, tid, ts=str(int(ts-ts1)), ctx=ctx, dev=dev)
                elif tag == 'stop' and key in spans:
                    end_span(key, duration)
                    if text == 'grabbing transaction lock':
                        ftext = 'holding transaction lock'
                        fkey = tid+'-'+ftext
                        new_span(ftext, fkey, tid, ts=str(int(ts-ts1)), ctx=ctx, dev=dev)
                        held_locks[tid] = ts
                elif tag == 'info' and text == 'releasing transaction lock':
                    ftext = 'holding transaction lock'
                    fkey = tid+'-'+ftext
                    if fkey in spans:
                        sts = held_locks.pop(tid)
                        fduration = ts-sts
                        end_span(fkey, fduration)
                for s, d in spans.values():
                    s.size = size
                for s in spans_running.values():
                    s.end = size
                span_duration.plain = f'Span {size:0.3f} s'
                if (args.follow or last-ts>0.1) and args.realtime:
                    live.refresh()
                if not args.follow and args.realtime and last:
                    delay = ts-last
                    if delay > 0:
                        time.sleep(delay/args.speedup)
                last = ts
            live.refresh()
        except Exception as e:
            print(f"ERROR: At line {n}")
            print(e)



def main(args):
    if args.setup:
        setup_progress_trace(args)
        sys.exit(0)
    if args.file is None:
        print("You must specify a file to process.")
        sys.exit(1)
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
