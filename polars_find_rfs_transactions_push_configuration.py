#!/usr/bin/env python3

import argparse
from datetime import datetime
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
    parser.add_argument('cmd', choices=['holding', 'applying', 'push', 'rfs', 'traceid-map'],
            help='Command to execute.')
    parser.add_argument('-o', '--output', type=str,
            help='File to write traceid-map result to.')
    return parser.parse_args(args)


def main(args):
    progress_trace = pl.scan_csv(args.file).with_columns(
        pl.col('TIMESTAMP').str.to_datetime("%Y-%m-%dT%H:%M:%S%.f")
    ).filter(
        (pl.col('DATASTORE') == 'running')
    )

    cols = [
        'TIMESTAMP',
        'DURATION',
        'TRANSACTION ID',
        'TRACE ID',
        'EVENT TYPE',
        'DATASTORE',
        'CONTEXT',
        'MESSAGE',
        'DEVICE',
        'NODE',
        'ANNOTATION']

    cols_with_start = cols.copy().append('START')

    d = progress_trace.filter(
        (pl.col('EVENT TYPE') == 'stop')
    ).select(cols).with_columns(
        (pl.col('TIMESTAMP')-(pl.col('DURATION')*1000000).cast(pl.Duration('us'))).alias('START')
    ).sort('TIMESTAMP')

    # ------------------------------------------------------------------------------------
    # Get transactions in CFS 

    # Find holding transaction lock events in CFS
    hold_events = d.filter(
        (pl.col('MESSAGE') == 'holding transaction lock') &
        (pl.col('NODE') == 'CFS')
    ).select(['TRANSACTION ID', 'TRACE ID', 'NODE', 'START', 'TIMESTAMP'])

    if args.tid:
        hold_events = hold_events.filter(
            (pl.col('TRANSACTION ID').is_in(list(map(int, args.tid.split(',')))))
        )

    if args.trid:
        hold_events = hold_events.filter(
            (pl.col('TRACE ID').is_in(list(map(int, args.tid.split(',')))))
        )

    if args.cmd == 'holding':
        print(hold_events.collect())
        return

    # Find surrounding applying transaction event
    trans_events = (d.join(hold_events, on=['TRANSACTION ID', 'TRACE ID'])
                     .group_by('TRANSACTION ID')
                     .last()
                     .select(
            ['TRANSACTION ID', 'TRACE ID', 'NODE', 'START', 'TIMESTAMP', 'ANNOTATION']))
    

    if args.cmd == 'applying':
        print(trans_events.collect())
        return

    push_events = d.filter(
        (pl.col('MESSAGE') == 'push configuration') &
        (pl.col('NODE') == 'CFS')
    ).select(['TRANSACTION ID', 'TRACE ID', 'NODE', 'DEVICE', 'START', 'TIMESTAMP'])
    
    push_in_trans_events = trans_events.join(push_events, on=['TRANSACTION ID', 'TRACE ID']).rename({
        'TIMESTAMP': 'TIMESTAMP_AT',
        'START': 'START_AT',
        'TIMESTAMP_right': 'TIMESTAMP_PUSH',
        'START_right': 'START_PUSH',
        'DEVICE': 'DEVICE_PUSH',    
    })

    if args.cmd == 'push':
        print(push_in_trans_events.collect())
        return

    # ------------------------------------------------------------------------------------
    # Find RFS applying transaction events that are within the same span as
    # the CFS push configuration events

    rfs_events = d.filter(
        (pl.col('MESSAGE') == 'applying transaction') &
        (pl.col('CONTEXT') == 'netconf') &
        (pl.col('NODE') != 'CFS') #&
        #(pl.col('DEVICE').is_not_null())
    ).select(['MESSAGE', 'NODE', 'DEVICE', 'START', 'TIMESTAMP','TRANSACTION ID', 'TRACE ID'])

    cfs_rfs_events = push_in_trans_events.join(rfs_events, left_on='DEVICE_PUSH', right_on='NODE').rename({
        'TIMESTAMP': 'TIMESTAMP_RFS',
        'START': 'START_RFS',
        'TIMESTAMP': 'TIMESTAMP_RFS',
        'DEVICE': 'DEVICE_RFS',    
    }).filter(
        ( pl.col('TIMESTAMP_RFS') <= pl.col('TIMESTAMP_PUSH') ) &
        ( pl.col('START_RFS') >= pl.col('START_PUSH') )
    )
    if args.cmd == 'rfs':
        print(cfs_rfs_events.collect())
              
    if args.cmd == 'traceid-map':
        traceid_map = cfs_rfs_events.select(['TRACE ID', 'TRACE ID_right']).rename({
            'TRACE ID': 'CFS TRACE ID',
            'TRACE ID_right': 'RFS TRACE ID'
        }).group_by('CFS TRACE ID', 'RFS TRACE ID').len().collect()
        if args.output:
            traceid_map.write_csv(args.output, separator=',')
        else:
            print(traceid_map)
    
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
    pl.Config().set_tbl_rows(100)
    pl.Config().set_tbl_cols(-1)
    pl.Config().set_fmt_str_lengths(64)

    main(parseArgs(sys.argv[1:]))
