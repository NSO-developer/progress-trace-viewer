#!/usr/bin/env python3

import argparse
import ast
from datetime import datetime
from functools import reduce
import re
import sys

import polars as pl


def parseArgs(args):
    parser = argparse.ArgumentParser()
    parser.add_argument('file', type=str,
        help='File to process.')
    parser.add_argument('-f', '--filter', type=str,
        help='Filter expression.')
    parser.add_argument('-s', '--sort', type=str,
        help='Sort column(s).')
    return parser.parse_args(args)


COLS_DTYPES = {
    'TRANSACTION ID': int,
    'SESSION ID': int,
    'TIMESTAMP': datetime.fromisoformat,
    'DURATION': float,
    'START': datetime.fromisoformat,
    'MIN': float,
    'MAX': float,
    'MEAN': float,
    'STD': float,
    'COUNT': int,
    'SUM': float,
    'len': int,
}


def parse_filter(filter_str):
    operators = {
        ast.Eq: pl.Expr.eq,
        ast.NotEq: pl.Expr.ne,
        ast.Lt: pl.Expr.lt,
        ast.Gt: pl.Expr.gt,
        ast.LtE: pl.Expr.le,
        ast.GtE: pl.Expr.ge,
        #ast.BitAnd: pl.Expr.and_,
        #ast.BitOr: pl.Expr.or_, 
        ast.Or: pl.Expr.or_,
        ast.And: pl.Expr.and_
    }
    switch = {
        ast.Lt: ast.Gt(),
        ast.Gt: ast.Lt(),
        ast.LtE: ast.GtE(),
        ast.GtE: ast.LtE()
    }

    def eval_expr(expr):
        return eval_(ast.parse(expr, mode='eval').body)

    def eval_(node):
        match node:
            case ast.Name(name):
                return pl.col(name.replace('_',' '))
            case ast.Constant(value):
                return value
            case ast.BinOp(left, op, right):
                return operators[type(op)](eval_(left), eval_(right))
            case ast.BoolOp(op, comparators):
                return reduce(lambda a,b: operators[type(op)](a,b), map(eval_, comparators))
            case ast.Compare(left, [op, *o], [right, *r]):
                # Special case for None comparison to handle is_null and is_not_null
                if type(op) == ast.Eq and right.value is None:
                    return eval_(left).is_null()
                elif type(op) == ast.NotEq and right.value is None:
                    return eval_(left).is_not_null()
                # Polars doesn't support contant on the left side
                # switch the left and right side and change the operator
                if type(left) == ast.Constant:
                    left, right = right, left
                    op = switch[type(op)]
                    switched = True
                expr = operators[type(op)](eval_(left), eval_(right))
                if len(o) > 0:
                    if not switched:
                        left = right
                    for op, right in zip(o, r):
                        switched = False
                        if type(left) == ast.Constant and type(right) == ast.Name:
                            left, right = right, left
                            op = switch[type(op)]
                            switched = True
                        expr = expr & (operators[type(op)](eval_(left), eval_(right)))
                        if not switched:
                            left = right
                print(expr)
                return expr
            case r:
                print("Unsupported:", r)
                raise TypeError(node)
            
    return eval_expr(filter_str)


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

    return duration_grouped_by_message.sort('MESSAGE')


def main(args):
    progress_trace = (pl.scan_csv(args.file)
        .filter(
             # filter out empty rows in case the CSV is not preprocessed
            (pl.col('TIMESTAMP') != '')
        )
    )

    pl.Config().set_tbl_rows(1000)
    pl.Config().set_fmt_str_lengths(100)

    statistics = get_statistics(progress_trace)

    if args.filter is not None:
        statistics = statistics.filter(parse_filter(args.filter))
    if args.sort is not None:
        statistics = statistics.sort(args.sort.split(','))

    print(statistics.collect().with_columns([
            sprintf(pl.col('COUNT'), "%6d", ),
            sprintf(pl.col('SUM'), "%12.6f"),
            sprintf(pl.col('STD'), "%12.6f"),
            sprintf(pl.col('MEAN'), "%12.6f"),
            sprintf(pl.col('MIN'), "%12.6f"),
            sprintf(pl.col('MAX'), "%12.6f")        
    ]))




if __name__ == '__main__':
    main(parseArgs(sys.argv[1:]))
