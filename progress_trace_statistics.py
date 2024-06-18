#!/usr/bin/env python3

import argparse
from datetime import datetime
import re
import sys

import polars as pl


def parseArgs(args):
    parser = argparse.ArgumentParser()
    parser.add_argument('file', type=str,
            help='File to process.')
    return parser.parse_args(args)


def sprintf(s, fmt):
    """
    Convert a polars series to string format

    Inputs:
      * s - string or expression
      * fmt - format specifier in fprint format, e.g. "%0.2f". Only specifiers of s, d, and f are supported.
              If specifier 's' is provided, alignment arguments of '>' and '<' are allowed:
                  '>5s' - right-align, width 5
                  '<5s' - left-align, width 5

    """
    # parse format
    parser = re.compile(r"^%(?P<pct>%?)(?P<align>[\<\>|]?)(?P<head>\d*)(?P<dot>\.?)(?P<dec>\d*)(?P<char>[dfs])$")
    result = parser.match(fmt)
    if not result:
        raise ValueError(f"Invalid format {fmt} specified.")

    # determine total width & leading zeros
    head = result.group("head")
    if head != '':
        total_width = int(head)
        lead_zeros = head[0] == '0'
    else:
        total_width = 0
        lead_zeros = False

    # determine # of decimals
    if result.group("char") == 's':
        # string requested: return immediately
        expr = s.str.ljust(total_width) if result.group("align") == '<' else s.str.pad_start(total_width)
        return pl.select(expr).to_series() if isinstance(s, pl.Series) else expr

    elif result.group("char") == 'd' or result.group("dot") != '.':
        num_decimals = 0
    else:
        num_decimals = int(result.group("dec"))

    # determine whether to display as percent
    if result.group("pct") == '%':
        s, pct = (s*100, [pl.lit('%')])
    else:
        s, pct = (s, [])

    if num_decimals > 0:
        # compute head portion
        head_width = max(0, total_width - num_decimals - 1)
        head = pl.when(s < 0).then(s.ceil()).otherwise(s.floor())

        # compute decimal portion
        decimal = (s-head)
        tail = [
            pl.lit('.'),
            (decimal*(10**num_decimals)).round(0).cast(pl.UInt64).cast(pl.Utf8).str.pad_start(num_decimals, '0')
        ]
        head = head.cast(pl.Int32).cast(pl.Utf8)
    else:
        # we only have head portion
        head_width = total_width
        head = s.cast(pl.Int32).cast(pl.Utf8)
        tail = []

    head = head.str.zfill(head_width) if lead_zeros else head.str.pad_start(head_width)
    expr = pl.concat_str([head, *tail, *pct])

    return pl.select(expr).to_series() if isinstance(s, pl.Series) else expr


def get_statistics(progress_trace, datastore='running'):
    progress_trace = progress_trace.filter(
            (pl.col('DATASTORE') == datastore) &
            (pl.col('EVENT TYPE') == 'stop') &
           ~(pl.col('MESSAGE').str.starts_with('check conflict') # filter out check conflict messages
                                                                 # as they contains dependant information
        )
    )

    duration_grouped_by_message = (progress_trace
        .group_by('MESSAGE')
        .agg([
            pl.col('MESSAGE').len().alias('COUNT'),
            pl.col('DURATION').sum().alias('SUM'),
            pl.col('DURATION').std().alias('STD'),
            pl.col('DURATION').mean().alias('MEAN'),
            pl.col('DURATION').min().alias('MIN'),
            pl.col('DURATION').max().alias('MAX')    
        ])
    )

    return duration_grouped_by_message.collect().sort('MESSAGE')


def main(args):
    progress_trace = (pl.scan_csv(args.file)
        .filter(
            (pl.col('TIMESTAMP') != '') # filter out empty rows in case the CSV is not preprocessed
        )
    )

    pl.Config().set_tbl_rows(1000)
    pl.Config().set_fmt_str_lengths(100)

    print(get_statistics(progress_trace).with_columns([
            sprintf(pl.col('COUNT'), "%6d", ),
            sprintf(pl.col('SUM'), "%12.6f"),
            sprintf(pl.col('STD'), "%12.6f"),
            sprintf(pl.col('MEAN'), "%12.6f"),
            sprintf(pl.col('MIN'), "%12.6f"),
            sprintf(pl.col('MAX'), "%12.6f")        
    ]))


if __name__ == '__main__':
    main(parseArgs(sys.argv[1:]))
