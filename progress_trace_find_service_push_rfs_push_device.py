#!/usr/bin/env python3

import argparse
from datetime import datetime
from functools import reduce
import re
import sys

import polars as pl


def parseArgs(args):
    parser = argparse.ArgumentParser()
    parser.add_argument('file', type=str,
        help='File to process.')
        # TODO: Use common framework for filtering
    parser.add_argument('--tid', type=str,
        help='Transaction id(s) to filter.')
    parser.add_argument('--trid', type=str,
        help='Trace id(s) to filter.')
    parser.add_argument('-d', '--debug', action='store_true',
        help='Debug output.')
    parser.add_argument('-f', '--filter', type=str,
        help='Filter expression.')
    parser.add_argument('-c', '--count', action='store_true',
        help='Count rows.')
    parser.add_argument('cmd', choices=
        [
            'holding',
            'applying',
            'push',
            'run-service',
            'rfs',
            'traceid-map',
            'rfs-run-service',
            'rfs-device', 
        ],
        help='Command to execute.')
    parser.add_argument('-o', '--output', type=str,
        help='File to write traceid-map result to.')
    parser.add_argument('--rows', type=int, default=50,
        help='Number of rows to display.')
    parser.add_argument('--simple', action='store_true',
        help='Use simple output format.')
    parser.add_argument('--duplicates', action='store_true',
        help='Show duplicated events.')
    return parser.parse_args(args)


def print_query(query, args, group_col='TRACE ID AT'):
    if args.count:
        query = query.group_by(group_col).len()
        if args.duplicates:
            query = query.filter(
                pl.col('len') > 1
            )
    if args.filter is not None:
        query = query.filter(args.filter_expr)
    print(query.collect())


def parse_filter(filter_str):
    for f in filter_str.split(','):
        exprs = []
        try:
            left, cmp, right = re.split('(==|>=|<=|>|<)', f)
            if not left.isidentifier():
                print(f"Invalid filter expression. Left part is not an identifier: {left}")
                sys.exit(1)
            if not right.isnumeric():
                print(f"Invalid filter expression. Right part is not numeric: {right}")
                sys.exit(1)
            right = int(right)
            if cmp == '==':
                exprs.append(pl.col(left) == right)
            elif cmp == '>=':
                exprs.append(pl.col(left) >= right)
            elif cmp == '<=':
                exprs.append(pl.col(left) <= right)
            elif cmp == '>':
                exprs.append(pl.col(left) > right)
            elif cmp == '<':
                exprs.append(pl.col(left) < right)
        except ValueError:
            print(f"Invalid filter expression: {f}")
            sys.exit(1)
    return reduce(lambda a,b: a&b, exprs)


def main(args):
    pl.Config().set_tbl_rows(args.rows)
    pl.Config().set_tbl_cols(-1)
    pl.Config().set_fmt_str_lengths(64)

    if args.filter is not None:
        args.filter_expr = parse_filter(args.filter)

    cols = [
        'TIMESTAMP',
        'DURATION',
        'TRANSACTION ID',
        'TRACE ID',
        'EVENT TYPE',
        'DATASTORE',
        'CONTEXT',
        'SERVICE',
        'MESSAGE',
        'DEVICE',
        'NODE',
        'ANNOTATION'
    ]
    cols_with_start = cols.copy().append('START')
    
    progress_trace = (pl.scan_csv(args.file)
        .with_columns(
            pl.col('TIMESTAMP').str.to_datetime("%Y-%m-%dT%H:%M:%S%.f")
        )
        .filter(
            (pl.col('DATASTORE') == 'running') &
            (pl.col('EVENT TYPE') == 'stop')
        )
        .select(cols)
        .with_columns(
            (pl.col('TIMESTAMP')-(pl.col('DURATION')*1000000).cast(pl.Duration('us'))).alias('START')
        )
        .sort('TIMESTAMP')
    )


    # ------------------------------------------------------------------------------------
    # Get transaction locks in CFS 
    # ------------------------------------------------------------------------------------

    # Find holding transaction lock events in CFS
    hold_events = (progress_trace.filter(
        (pl.col('MESSAGE') == 'holding transaction lock') &
        (pl.col('NODE') == 'CFS')
    ).select(['TRANSACTION ID', 'TRACE ID', 'START', 'TIMESTAMP'])
     .rename(lambda column_name: column_name+' HTL')
    )

    if args.tid:
        hold_events = hold_events.filter(
            (pl.col('TRANSACTION ID HTL').is_in(list(map(int, args.tid.split(',')))))
        )

    if args.trid:
        hold_events = hold_events.filter(
            (pl.col('TRACE ID HTL').is_in(list(args.trid.split(','))))
        )

    # COMMAND: holding

    if args.cmd == 'holding':
        print(hold_events.collect())
        return


    # ------------------------------------------------------------------------------------
    # Find surrounding applying transaction events in CFS
    # ------------------------------------------------------------------------------------

    # Find surrounding applying transaction events query

    trans_events = (progress_trace.rename(lambda column_name: column_name+' AT')
        .join(hold_events,
            left_on=['TRANSACTION ID AT', 'TRACE ID AT'], 
            right_on=['TRANSACTION ID HTL', 'TRACE ID HTL']
        )
        .group_by('TRANSACTION ID AT')
        .last()
        .select(['TRANSACTION ID AT', 'TRACE ID AT', 'START AT', 'TIMESTAMP AT', 'ANNOTATION AT'])
    )
    
    # COMMAND: applying

    if args.cmd == 'applying':
        print(trans_events.collect())
        return


    # ------------------------------------------------------------------------------------
    # Find contained run service events in CFS
    # ------------------------------------------------------------------------------------

    # Find contained run service events query

    rs_events = (progress_trace.rename(lambda column_name: column_name+' RS')
        .filter(
            (pl.col('MESSAGE RS') == 'run service') &
            (pl.col('NODE RS') == 'CFS')
        )
        .select(['TRANSACTION ID RS', 'TRACE ID RS', 'SERVICE RS', 'START RS', 'TIMESTAMP RS'])
    )

   # Correlate push configuration events with applying transaction events

    rs_in_trans_events = (trans_events
        .join(rs_events, how='left',
            left_on=['TRANSACTION ID AT', 'TRACE ID AT'],
            right_on=['TRANSACTION ID RS', 'TRACE ID RS']
        )
    )

    # COMMAND: run-service

    if args.cmd == 'run-service':
        print_query(rs_in_trans_events, args)
        return


    # ------------------------------------------------------------------------------------
    # Find contained push configuration events in CFS
    # ------------------------------------------------------------------------------------

    # Find contained push configuration events query

    push_events = (progress_trace.rename(lambda column_name: column_name+' PC')
        .filter(
            (pl.col('MESSAGE PC') == 'push configuration') &
            (pl.col('NODE PC') == 'CFS')
        )
        .select(['TRANSACTION ID PC', 'TRACE ID PC', 'DEVICE PC', 'START PC', 'TIMESTAMP PC'])
    )

    # Correlate push configuration events with applying transaction events

    push_in_trans_events = (trans_events
        .join(push_events,
            left_on=['TRANSACTION ID AT', 'TRACE ID AT'],
            right_on=['TRANSACTION ID PC', 'TRACE ID PC']
        )
    )

    # COMMAND: push

    if args.cmd == 'push':
        print_query(push_in_trans_events, args)
        return


    # ------------------------------------------------------------------------------------
    # Find RFS applying transaction events that are within the same span as
    # the CFS push configuration events
    # ------------------------------------------------------------------------------------

    # Find RFS applying transaction events query

    rfs_events = (progress_trace.rename(lambda column_name: column_name+' RAT')
        .filter(
            (pl.col('MESSAGE RAT') == 'applying transaction') &
            (pl.col('CONTEXT RAT') == 'netconf') &
            (pl.col('NODE RAT') != 'CFS'
            #(pl.col('DEVICE').is_not_null())
        )
    ).select(['NODE RAT', 'DEVICE RAT', 'START RAT', 'TIMESTAMP RAT','TRANSACTION ID RAT', 'TRACE ID RAT'])
    )

    # Correlate RFS applying transaction events with CFS push configuration events

    cfs_rfs_events = (push_in_trans_events
        .join(rfs_events, how='left',
              left_on='DEVICE PC',
              right_on='NODE RAT'
        )
        .filter(
            ( pl.col('TIMESTAMP RAT') <= pl.col('TIMESTAMP PC') ) &
            ( pl.col('START RAT') >= pl.col('START PC') )
        )
    )

    # COMMAND: rfs

    if args.cmd == 'rfs':
        print_query(cfs_rfs_events, args)
        return

    # COMMAND: traceid-map

    if args.cmd == 'traceid-map':
        traceid_map = (cfs_rfs_events
            .select(['TRACE ID AT', 'TRACE ID RAT'])
            .group_by('TRACE ID AT', 'TRACE ID RAT').len().collect()
        )
        if args.output:
            traceid_map.write_csv(args.output, separator=',')
        else:
            print(traceid_map)
        return

    # ------------------------------------------------------------------------------------
    # Find contained run service events in RFS
    # ------------------------------------------------------------------------------------

    # Find contained run service events query

    rrs_events = (progress_trace.rename(lambda column_name: column_name+' RRS')
        .filter(
            (pl.col('MESSAGE RRS') == 'run service') &
            (pl.col('NODE RRS') != 'CFS')
        )
        .select(['TRANSACTION ID RRS', 'TRACE ID RRS', 'SERVICE RRS', 'START RRS', 'TIMESTAMP RRS'])
    )

   # Correlate push configuration events with applying transaction events

    rrs_in_trans_events = (cfs_rfs_events
        .join(rrs_events, how='left',
            left_on=['TRANSACTION ID RAT', 'TRACE ID RAT'],
            right_on=['TRANSACTION ID RRS', 'TRACE ID RRS']
        )
    )

    # COMMAND: rfs-run-service

    if args.cmd == 'rfs-run-service':
        print_query(rrs_in_trans_events, args)
        return
    

    # ------------------------------------------------------------------------------------
    # Find devices that were pushed during a CFS push configuration
    # ------------------------------------------------------------------------------------

    rfs_push_events = (progress_trace.rename(lambda column_name: column_name+' RPC')
        .filter(
            (pl.col('MESSAGE RPC') == 'push configuration') &
            (pl.col('NODE RPC') != 'CFS')
        )
        .select(['TRACE ID RPC', 'NODE RPC', 'DEVICE RPC', 'START RPC', 'TIMESTAMP RPC'])
    )
    rfs_device_events = (cfs_rfs_events
        .join(rfs_push_events, how='left',
             left_on=['TRACE ID RAT', 'DEVICE PC'],
             right_on=['TRACE ID RPC', 'NODE RPC']
        )
    )

    if args.simple:
        rfs_devices = rfs_device_events.select([
            'TRANSACTION ID AT',
            'TRACE ID AT',
            'START AT',
            'TIMESTAMP AT',
            'DEVICE PC',
            'TRACE ID RAT',
            'START RAT',
            'TIMESTAMP RAT',
            'START RPC',
            'TIMESTAMP RPC',
            'DEVICE RPC'
        ])

    # COMMAND: rfs-device

    if args.cmd == 'rfs-device':
        print_query(rfs_device_events, args, ['TRACE ID AT', 'DEVICE PC'])
        return
    
    print("ERROR: Command not implemented: " + args.cmd)


    # ------------------------------------------------------------------------------------
    # Find duplicated RFS applying transaction events during a CFS push configuration
    # Most likely cause is that a dry-run was performed before the actual push

    # duplicated_cfs_rfs_events = cfs_rfs_events.group_by('TRANSACTION ID').len().filter(
    #     pl.col('len') > 1
    # )
    # print(duplicated_cfs_rfs_events.collect())

    # duplicated_cfs_rfs_events = cfs_rfs_events.group_by('TRACE ID').len().filter(
    #     pl.col('len') > 1
    # )
    # print(duplicated_cfs_rfs_events.collect())


if __name__ == '__main__':
    main(parseArgs(sys.argv[1:]))
