#!/usr/bin/env python3

import argparse
from datetime import datetime
import sys

import pandas


def parseArgs(args):
    parser = argparse.ArgumentParser()
    parser.add_argument('file', type=str,
            help='File to process.')
    return parser.parse_args(args)


def get_statistics(progress_trace, datastore='running'):
    if datastore == '':
        d = progress_trace[(progress_trace.DATASTORE.isnull()) &
                    (progress_trace.EVENTTYPE == 'stop') &
                    ~(progress_trace.MESSAGE.str.startswith('check conflict'))]
    else:
        d = progress_trace[(progress_trace.DATASTORE == datastore) &
                    (progress_trace.EVENTTYPE == 'stop') &
                    ~(progress_trace.MESSAGE.str.startswith('check conflict')) ]

    duration_grouped_by_message = d.groupby('MESSAGE').DURATION
    v_cnt = duration_grouped_by_message.count()
    v_cnt.name = "COUNT"
    v_sum = duration_grouped_by_message.sum()
    v_sum.name = "SUM"
    v_sd = duration_grouped_by_message.std()
    v_sd.name = "STD"
    v_mean = duration_grouped_by_message.mean()
    v_mean.name = "MEAN"
    v_min = duration_grouped_by_message.min()
    v_min.name = "MIN"
    v_max = duration_grouped_by_message.max()
    v_max.name = "MAX"

    return pandas.concat([v_cnt, v_sum, v_sd, v_mean, v_min, v_max], axis=1)


def main(args):
    progress_trace = pandas.read_csv(args.file, index_col='TIMESTAMP',
                                     dtype={'DATASTORE': 'string'})
    progress_trace.rename(columns={'EVENT TYPE': 'EVENTTYPE'}, inplace=True)

    print("=== RUNNING ===")
    print(get_statistics(progress_trace))
    print("\n=== NO DATASTORE ===")
    print(get_statistics(progress_trace, ''))
    print("\n=== OPERATIONAL ===")
    print(get_statistics(progress_trace, 'operational'))

if __name__ == '__main__':
    main(parseArgs(sys.argv[1:]))
