#!/usr/bin/env python3

"""A simple progress trace viewer example. See the NSO Observability
Exporter tools for more.
"""

# TODO:
# - Add follow.
# - Create specifig progress trace reader. ✅
#   - Handle different versions and empty fields.
#   - Handle empty timestamps.
#   - Handle device names. Filter on device.
#   - Handle spans.
#   - Handle service names/types.
# - Filters:
#   - Add begin and end timestamp filters.
#   - Add transaction id filter.


import argparse
import csv
from datetime import datetime
from os import access, R_OK, SEEK_END
from os.path import exists, isfile
import sys
from time import sleep
from rich import print as rprint
from rich.bar import Bar
from rich.color import Color
from rich.console import Console, Group
from rich.live import Live
from rich.style import Style
from rich.table import Table
from rich.text import Text


def parseArgs(args=None):
    parser = argparse.ArgumentParser()
    parser.add_argument('-f', '--follow', action='store_true', default=False,
            help="Follow file and graph as traces come.")
    parser.add_argument('file', type=str, nargs='?',
            help='File to process.')
    # Filter options
    parser.add_argument('--oper', action='store_true', default=False,
            help='Graph operational transactions.')
#    parser.add_argument('--events', type=str,
#            help='Read events to filter from file.')
#    parser.add_argument('--tid', type=str,
#            help='Filter on transaction id(s).')
#    parser.add_argument('--ctid', type=str,
#            help='Color transaction id(s).')
#    parser.add_argument('-b', '--begin', type=str,
#            help='Start timestamp')
#    parser.add_argument('-e', '--end', type=str,
#            help='End timestamp')
#    parser.add_argument('--write', type=str,
#            help='Write the progress trace events to file.')
#    parser.add_argument('--realtime', action='store_true', default=False,
#            help='Skip view updates and sleeps during parsing.')
#    parser.add_argument('--speedup', type=int, default=1,
#            help='Speedup realtime view n times.')
#    parser.add_argument('-t', '--timestamp', action='store_true', default=False,
#            help='Show start timestamp.')

    parser.add_argument('--version', action='version', version='%(prog)s 0.1')
    parser.add_argument('--detect', action='store_true', default=False,
            help='Detect NSO version producing progress trace.')
    return parser.parse_args(args)


class nso_progress_trace(csv.Dialect):
    """Describe the usual properties of NSO-generated progress trace CSV files."""
    delimiter = ','
    quotechar = '"'
    doublequote = True
    skipinitialspace = False
    lineterminator = '\n'
    quoting = csv.QUOTE_MINIMAL
csv.register_dialect("nso_progress_trace", nso_progress_trace)


class ProgressTraceReader:
    def __init__(self, f, *args, **kwds):
        self.f = f
        self.reader = csv.reader(f, dialect='nso_progress_trace')
        self.capabilities, self.fieldnames, self.version = \
            detect_pt_capabilities(self.reader)
        # if self.capabilities is None:
        #     raise RuntimeError("Couldn't detect progress trace capabilities.")

    def __iter__(self):
        return self

    def __next__(self):
        return next(self.reader)
    

def detect_pt_capabilities(csvreader):
    try:
        fieldnames = { n: p for p,n in enumerate(next(csvreader)) }
        capabilities = None
        version = None
        if 'TIMESTAMP' in fieldnames:
            version = '-5.3'
            capabilities = set()        
            if 'EVENT TYPE' in fieldnames:
                capabilities.add('duration') # Supported in version 5.4-
                version = '5.4-5.6'
            if 'TRACE ID' in fieldnames:
                capabilities.add('traces') # Supported in version 5.7-
                version = '5.7-6.0'
            if 'SPAN ID' in fieldnames:
                capabilities.add('spans') # Supported in version 6.1-
                version = '6.1-'
        return capabilities, fieldnames, version
    except StopIteration:
        return None, None, None


def graph_progress_trace(args, csvreader, capabilities, fieldnames):
    oper = args.oper

    color_numbers = list(
                      filter(
                        lambda i: not i in [4, 16, 17, 18],
                        [i for i in range(1, 232)]))
    begin = 0.0
    size = 0.0
    bars = Group()
    spans = {}
    tids_color = {}

    # Set column positions as local variables
    event_num = ts_num = dur_num = trid_num = msg_num = span_num =\
    pspan_num = sess_num = tid_num = ds_num = srv_num = attr_num = False
    if 'EVENT TYPE' in fieldnames:
        event_num = fieldnames['EVENT TYPE']
    if 'TIMESTAMP' in fieldnames:
        ts_num = fieldnames['TIMESTAMP']
    if 'DURATION' in fieldnames:
        dur_num = fieldnames['DURATION']
    if 'TRACE ID' in fieldnames:
        trid_num = fieldnames['TRACE ID']
    if 'SPAN ID' in fieldnames:
        span_num = fieldnames['SPAN ID']
    if 'PARENT SPAN ID' in fieldnames:
        pspan_num = fieldnames['PARENT SPAN ID']
    if 'SESSION ID' in fieldnames:
        sess_num = fieldnames['SESSION ID']
    if 'TRANSACTION ID' in fieldnames:
        tid_num = fieldnames['TRANSACTION ID']
    if 'DATASTORE' in fieldnames:
        ds_num = fieldnames['DATASTORE']
    if 'MESSAGE' in fieldnames:
        msg_num = fieldnames['MESSAGE']
    if 'SERVICE' in fieldnames:
        srv_num = fieldnames['SERVICE']
    if 'ATTRIBUTE VALUE' in fieldnames:
        attr_num = fieldnames['ATTRIBUTE VALUE']
 
    table = Table(title=f'Progress Trace {args.file}')
    table.add_column("Trace ID", width=12, no_wrap=True)
    table.add_column("Event [Transaction ID]", min_width=40, max_width=60)
    table.add_column("Duration", min_width=8, no_wrap=True)
    span_duration = Text(" Span 0.0 ms")
    table.add_column(span_duration,
                     max_width=(int(Console().width)-12-60-8-15),
                     no_wrap=True)

    def follow(reader):
        '''generator function that yields new lines in a file as they are written'''
        while True:
            try:
                yield next(reader)
            except StopIteration:
                sleep(0.1)

    if args.follow:
        csvreader.f.seek(0, SEEK_END)
        reader = follow(csvreader)
    else:
        reader = csvreader

    with Live(table, auto_refresh=False) as live:
        for l in reader:
            event_type = l[event_num]
            if event_type == '':
                # Skip empty lines, for now (attibute values)
                continue
            if not oper and l[ds_num] == 'operational':
                # Skip operational transactions, for now
                continue
            ts = datetime.fromisoformat(l[ts_num]).timestamp()
            duration = float(l[dur_num]) if l[dur_num] else 0.0

            trid = l[trid_num][-12:] if trid_num else ''
            span = l[span_num] if span_num else ''
            pspan = l[pspan_num] if pspan_num else ''
            sid = l[sess_num] if sess_num else ''
            tid = l[tid_num] if tid_num else ''
            msg = l[msg_num] if msg_num else ''
            srv = l[srv_num] if srv_num else ''
            attr = l[attr_num] if attr_num else ''
            # Can SPAN ID used as key, when available?
            key = f'{trid}{span}{pspan}{sid}{tid}{srv}{attr}{msg}'

            if begin == 0.0:
                begin = ts
            size = ts - begin
            if event_type == 'start':
                if trid not in tids_color:
                    color = Color.from_ansi(color_numbers.pop(0))
                    tids_color[trid] = color
                else:
                    color=tids_color[trid]
                span = Bar(begin=size, end=size, size=size, color=color)
                desc = Text('', style=Style(color=color))
                rtid = Text(trid, style=Style(color=color))
                rtext= Text(f'{msg} {tid}', style=Style(color=color))
                spans[key] = span, desc
                table.add_row(rtid, rtext, desc, span)
            elif event_type == 'stop' and key in spans:
                span, desc = spans[key]
                desc.append(f'{duration*1000:0.3f}')
                span.end = span.end + duration
                for s, _ in spans.values():
                    s.size = size
                span_duration.plain = f'Span {size*1000:0.3f} ms'
            else:
                continue
            if args.follow:
                live.refresh()



def main(args):
    if args.file is None:
        print("ERROR: No file provided.")
        sys.exit(1)
    if not exists(args.file):
        print(f"ERROR: {args.file} does not exist.")
        sys.exit(1)
    if not isfile(args.file):
        print(f"ERROR: {args.file} is not a file.")
        sys.exit(1)
    if not access(args.file, R_OK):
        print(f"ERROR: Permission denied to read {args.file}.")
        sys.exit(1)
    with open(args.file, 'r') as csvfile:
        reader = ProgressTraceReader(csvfile)
        if args.detect:
            print(f"Detected NSO progress trace version: {reader.version}")
            print(f"Capabilities: {reader.capabilities}")
            sys.exit(0)
        if reader.version == '-5.3':
            print("ERROR: Progress trace version -5.3 is not supported.")
            sys.exit(1)
        if reader.capabilities is None:
            print("ERROR: Couldn't detect progress trace capabilities.")
            sys.exit(1)
        graph_progress_trace(args, reader, reader.capabilities, reader.fieldnames)


if __name__ == '__main__':
    main(parseArgs())