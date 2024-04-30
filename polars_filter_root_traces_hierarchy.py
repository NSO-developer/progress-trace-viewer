#!/usr/bin/env python3

import argparse
import csv
from datetime import datetime
import sys

import polars as pl
import polars.selectors as cs


def parseArgs(args):
    parser = argparse.ArgumentParser()
    parser.add_argument('file', type=str,
            help='File to process.')
    parser.add_argument('span', type=str,
            help='Root span id.')
    return parser.parse_args(args)


def main(args):
    progress_trace = (pl.scan_csv(args.file)
                .filter((pl.col('TIMESTAMP') != '') &
                        pl.col('EVENT TYPE').str.contains('start|stop'))
                .with_row_index()
    )

    #pl.Config().set_tbl_rows(1000)

    # Find the row for the root span
    df = progress_trace.collect(streaming=True)
    start_index = df.row(
            by_predicate=(
                (pl.col('SPAN ID') == args.span) &
                (pl.col('EVENT TYPE') == 'start')
            ),
            named=True
    )['index']

    result_df = df.clone()
    
    csvfile = open('output.csv', 'w')
    schema = progress_trace.schema
    del schema['index']
    writer = csv.DictWriter(csvfile, fieldnames=schema.keys(), extrasaction='ignore')
    writer.writeheader()

    contained_spans = set()
    contained_spans.add(args.span)
    contained_trans = set()

    for row in progress_trace.collect(
                   streaming=True
               )[start_index:].iter_rows(named = True):
        event_type = row['EVENT TYPE']
        span = row['SPAN ID']
        parent_span = row['PARENT SPAN ID']
        trans = row['TRANSACTION ID']

        if parent_span in contained_spans:
            contained_spans.add(span)
            contained_trans.add(trans)
            print(row.values())
            writer.writerow(row)
        elif span in contained_spans:
            contained_trans.add(trans)
            writer.writerow(row)
            print(row.values())
        elif trans in contained_trans:
            writer.writerow(row)
            print(row.values())

        if span == args.span and parent_span is None and event_type == 'stop':
            break

if __name__ == '__main__':
    main(parseArgs(sys.argv[1:]))
