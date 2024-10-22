#!/usr/bin/env python3

"""A simple progress trace viewer example. See the NSO Observability
Exporter tools for more.
"""

# TODO:
# - Create specifig progress trace reader.
#   - Handle different versions and empty fields.
# - Add follow.
# - Add begin and end timestamp filters.
# - Add transaction id filter.
# - Handle empty timestamps.
# - Handle device names. Filter on device.
# - Handle operational transactions.
# - Handle spans.ß
# - Handle service names/types.


import argparse
import csv
from datetime import datetime
from os import access, R_OK
from os.path import exists, isfile
import sys
from rich.bar import Bar
from rich.console import Group
from rich.table import Table
from rich.text import Text
from rich.color import Color
from rich.style import Style
from rich.console import Console
from rich import print


def parseArgs(args=None):
    parser = argparse.ArgumentParser()
    parser.add_argument('-f', '--follow', action='store_true', default=False,
            help="Follow file and graph as traces come.")
    parser.add_argument('file', type=str, nargs='?',
            help='File to process.')

    parser.add_argument('--oper', action='store_true', default=False,
            help='Graph operational transactions.')
    
    
    parser.add_argument('--events', type=str,
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


class nso_progress_trace(csv.Dialect):
    """Describe the usual properties of NSO-generated progress trace CSV files."""
    delimiter = ','
    quotechar = '"'
    doublequote = True
    skipinitialspace = False
    lineterminator = '\n'
    quoting = csv.QUOTE_MINIMAL
csv.register_dialect("nso_progress_trace", nso_progress_trace)


def detect_pt_capabilities(csvreader):
    try:
        fieldnames = { n: p for p,n in enumerate(next(csvreader)) }
        capabilities = set()
        if 'EVENT TYPE' in fieldnames:
            capabilities.add('start') # Supported in version 5.4-
        if 'TRACE ID' in fieldnames:
            capabilities.add('traces') # Supported in version 5.7-
        if 'SPAN ID' in fieldnames:
            capabilities.add('spans') # Supported in version 6.1-
        if not capabilities:
            return None, None
        return capabilities, fieldnames
    except StopIteration:
        return None, None


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
    event_num = ts_num = dur_num = tid_num = msg_num = span_num = trace_num =\
    pspan_num = sess_num = tid_num = ds_num = srv_num = attr_num = False
    if 'EVENT TYPE' in fieldnames:
        event_num = fieldnames['EVENT TYPE']
    if 'TIMESTAMP' in fieldnames:
        ts_num = fieldnames['TIMESTAMP']
    if 'DURATION' in fieldnames:
        dur_num = fieldnames['DURATION']
    if 'TRACE ID' in fieldnames:
        trace_num = fieldnames['TRACE ID']
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

    for l in csvreader:
        if l[0] == '': # Skip empty lines, for now
            continue
        if not oper and l[ds_num] == 'operational':
            continue
        event_type = l[event_num]
        ts = datetime.fromisoformat(l[ts_num]).timestamp()
        duration = float(l[dur_num]) if l[dur_num] else 0.0

        trid = l[trace_num][-12:] if trace_num else ''
        span = l[span_num] if span_num else ''
        pspan = l[pspan_num] if pspan_num else ''
        sid = l[sess_num] if sess_num else ''
        tid = l[tid_num] if tid_num else ''
        msg = l[msg_num] if msg_num else ''
        srv = l[srv_num] if srv_num else ''
        attr = l[attr_num] if attr_num else ''
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
            d = Text('', style=Style(color=color))
            rtid = Text(trid, style=Style(color=color))
            rtext= Text(f'{msg} {tid}', style=Style(color=color))
            spans[key] = span, d
            table.add_row(rtid, rtext, d, span)
        elif event_type == 'stop' and key in spans:
            s, d = spans[key]
            d.append(f'{duration*1000:0.3f}')
            s.end = size
        else:
            continue

    for s, _ in spans.values():
        s.size = size
    span_duration.plain = f'Span {size*1000:0.3f} ms'
    console = Console()
    print(table, flush=True)


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
        reader = csv.reader(csvfile, dialect='nso_progress_trace')
        capabilities, fieldnames = detect_pt_capabilities(reader)
        if capabilities is None:
            print(f"ERROR: {args.file} Couldn't detect column names.")
            sys.exit(1)
        graph_progress_trace(args, reader, capabilities, fieldnames)


if __name__ == '__main__':
    main(parseArgs())