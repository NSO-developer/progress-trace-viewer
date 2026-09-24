import argparse
from datetime import datetime
import sys

import polars as pl


def parseArgs(args):
    parser = argparse.ArgumentParser()
    parser.add_argument('file', type=str,
            help='File to process.')
    parser.add_argument('-s', '--span', type=str,
            help='Span ID')
    return parser.parse_args(args)

def main(progress_trace, span):
    # TODO: Add whatever's done below with with row index
    #progress_trace = pl.scan_parquet(args.file).with_row_index(name="n", offset=1)
    root_span = progress_trace.filter(pl.col('SPAN ID') == span).collect()
    parent_span_id = root_span.item(0, 'PARENT SPAN ID')
    parent_spans = progress_trace.filter(pl.col('SPAN ID') == parent_span_id).collect()

    df = pl.concat([parent_spans, root_span])
    return df

def ancestors(pt, span_id):
    span = pt.filter(pl.col('SPAN ID') == span_id).collect()
    psi = span.item(0, 'PARENT SPAN ID')
    n = span.item(0, 'n')
    
    upper_df = pt.slice(0, n + 1)
    parents = upper_df.filter(pl.col('SPAN ID') == span_id)

    df = pl.concat([parents, span])
    for p in parents.iter_rows(named = True):
        ancestors(upper_df, p['SPAN ID'])

def descendants():
    pass

if __name__ == '__main__':
    args = parseArgs(sys.argv[1:])
    progress_trace = pl.scan_csv(args.file)
    result = main(progress_trace, args.span)
    
    with pl.Config(tbl_cols=-1):
      print(result)