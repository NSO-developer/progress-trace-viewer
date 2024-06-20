#!/usr/bin/env python3
#-*- coding: utf-8; mode: python; py-indent-offset: 4; tab-width: 4 -*-

import argparse
import ast
from datetime import datetime
from functools import reduce
import re
import sys
import time

import polars as pl

def parseArgs(args):
    parser = argparse.ArgumentParser()
    parser.add_argument('file', type=str,
            help='File to process.')
    parser.add_argument('--tid', type=str,
            help='Transaction id(s) to filter.')
    parser.add_argument('--trid', type=str,
            help='Trace id(s) to filter.')
    parser.add_argument('--et', type=str,
            help='Event type(s) to filter.')
    parser.add_argument('--msg', type=str,
            help='Message(s) to filter.')
    parser.add_argument('--ctx', type=str,
            help='Context(s) to filter.')
    parser.add_argument('--ds', type=str,
            help='Datastore to filter.')
    parser.add_argument('--device', type=str,
            help='Device(s) to filter.')
    parser.add_argument('--node', type=str,
            help='Node(s) to filter.')
    parser.add_argument('--service', type=str,
            help='Service(s) to filter.')
    parser.add_argument('--ann', type=str,
            help='Annotation(s) to filter.')
    parser.add_argument('--dur', type=str,
            help='Duration.')
    parser.add_argument('--begin', type=str,
            help='Beginning timestamp.')
    parser.add_argument('--end', type=str,
            help='Ending timestamp.')
    parser.add_argument('-f', '--filter', type=str,
            help='Filter expression.')
    parser.add_argument('-m', '--mincols', action='store_true',
            help='Less number of columns.')
    parser.add_argument('-n', '--nodyncols', action='store_true',
            help='Remove dynamic columns (Useful for comparing files for different runs).')
    parser.add_argument('-o', '--output', type=str,
            help='File to write result to.')
    parser.add_argument('-s', '--start', action='store_true',
            help='Calculate start timestamp from timestamp end duration.')
    parser.add_argument('--rows', type=int, default=50,
            help='Number of rows to display.')
    parser.add_argument('-v', '--verbose', action='store_true',
            help='Verbose output.')
    parser.add_argument('--group', type=str,
            help='Column(s) to group on.')

    return parser.parse_args(args)


COLS_DTYPES = {
    'TRANSACTION ID': int,
    'SESSION ID': int,
    'TIMESTAMP': datetime.fromisoformat,
    'DURATION': float,
    'START': datetime.fromisoformat,
    'len': int,
    'MIN:': float,
    'MAX:': float,
    'MEAN:': float,
    'STD:': float,
    'COUNT:': int,
}


def create_filter(name, arg):
        exprs = []
        pos = []
        neg = []
        dtype = COLS_DTYPES.get(name, str)
        for s in arg.split(','):
            if not s.startswith('^'):
                pos.append(s)
            else:
                neg.append(s[1:])
        if len(pos):
            if pos[0] != '~':
                exprs.append((pl.col(name).is_in(list(map(dtype, pos)))))
            else:
                exprs.append((pl.col(name).is_null()))
        if len(neg):
            if neg[0] != '~':
                exprs.append(~(pl.col(name).is_in(list(map(dtype, neg)))))
            else:
                exprs.append(pl.col(name).is_not_null())
        filter = [reduce(lambda a,b: a&b, exprs)]
        return filter


# Use the ast module to parse the filter expression

def parse_filter(expr):
    operators = {
        ast.Eq: pl.Expr.eq,
        ast.NotEq: pl.Expr.ne,
        ast.Lt: pl.Expr.lt,
        ast.Gt: pl.Expr.gt,
        ast.LtE: pl.Expr.le,
        ast.GtE: pl.Expr.ge,
        ast.Or: pl.Expr.or_,
        ast.And: pl.Expr.and_
    }
    switch = {
        ast.Lt: ast.Gt(),
        ast.Gt: ast.Lt(),
        ast.LtE: ast.GtE(),
        ast.GtE: ast.LtE()
    }

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
            case ast.Compare(left, ops, rights):
                # Special case for None comparison to handle is_null and is_not_null
                if len(ops) == 1:
                    op, right = ops[0], rights[0]
                    if type(op) == ast.Eq and right.value is None:
                        return eval_(left).is_null()
                    elif type(op) == ast.NotEq and right.value is None:
                        return eval_(left).is_not_null()
                # Polars doesn't support contant on the left side
                # switch the left and right side and change the operator
                exprs = []
                for op, right in zip(ops, rights):
                    switched = False
                    if type(left) == ast.Constant and type(right) == ast.Name:
                        left, right = right, left
                        op = switch[type(op)]
                        switched = True
                    exprs.append(operators[type(op)](eval_(left), eval_(right)))
                    if not switched:
                        left = right
                # And all the expressions together and filter out True
                return reduce(lambda a,b: a & b, filter(lambda a: a is not True, exprs))
            case r:
                print("Unsupported:", r)
                raise TypeError(node)
            
    return eval_(ast.parse(expr, mode='eval').body)


def main_polars(args):
    progress_trace = pl.scan_csv(args.file).\
        with_columns(pl.col('TIMESTAMP').str.to_datetime("%Y-%m-%dT%H:%M:%S%.f")).\
        sort('TIMESTAMP')

    if args.start is not None:
        progress_trace = progress_trace.with_columns(
            (pl.col('TIMESTAMP')-(pl.col('DURATION')*1000000).cast(pl.Duration('us'))).alias('START')
        )

    filter_expr = []
    if args.tid is not None:        
        filter_expr += create_filter('TRANSACTION ID', args.tid)
    if args.trid is not None:
        filter_expr += create_filter('TRACE ID', args.trid)
    if args.et is not None:
        filter_expr += create_filter('EVENT TYPE', args.et)
    if args.msg is not None:    
        filter_expr += create_filter('MESSAGE', args.msg)
    if args.ctx is not None:
        filter_expr += create_filter('CONTEXT', args.ctx)
    if args.ds is not None:
        filter_expr += create_filter('DATASTORE', args.ds)
    if args.device is not None:    
        filter_expr += create_filter('DEVICE', args.device)
    if args.node is not None:    
        filter_expr += create_filter('NODE', args.node)
    if args.service is not None:
        filter_expr += create_filter('SERVICE', args.service)
    if args.ann is not None:
        filter_expr += create_filter('ANNOTATION', args.ann)
    if args.dur is not None:
        filter_expr += create_filter('DURATION', args.ann)
    if args.begin is not None:
        filter_expr += (pl.col('TIMESTAMP') >= datetime.fromisoformat(args.begin))
    if args.end is not None:
        filter_expr += (pl.col('TIMESTAMP') <= datetime.fromisoformat(args.end))
    if args.filter is not None:
        filter_expr.append(parse_filter(args.filter))
    if len(filter_expr):
        progress_trace = progress_trace.filter(reduce(lambda a,b: a&b, filter_expr))

    if args.group is not None:
        progress_trace = progress_trace.group_by(args.group.split(',')).len()

    if args.verbose:
        print(progress_trace)

    result = progress_trace.collect()
    columns = progress_trace.columns

    if args.mincols:
        columns = [
            'EVENT TYPE',
            'MESSAGE',
            'ANNOTATION',
            'CONTEXT',
            'TIMESTAMP',
            'DURATION',
            'TRANSACTION ID',
            'TRACE ID', 
            'NODE', 
            'DEVICE',
            'SERVICE',
        ]
        if args.start:
            columns.append('START')

    if args.nodyncols:
        columns = [
            'EVENT TYPE',
            'MESSAGE',
            'ANNOTATION',
            'CONTEXT',
            'NODE', 
            'DEVICE', 
        ]

    result = result.select(columns)
    pl.Config().set_tbl_rows(args.rows)

    if args.output:
        result.write_csv(args.output, separator=',')
    else:
        with pl.Config(tbl_cols=-1, fmt_str_lengths=100):
            print(result)

if __name__ == '__main__':
    main_polars(parseArgs(sys.argv[1:]))
