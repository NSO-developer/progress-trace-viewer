#!/usr/bin/env python3
#-*- coding: utf-8; mode: python; py-indent-offset: 4; tab-width: 4 -*-

import argparse
from datetime import datetime
import sys

import polars as pl
import polars.selectors as cs


def parseArgs(args):
    parser = argparse.ArgumentParser()
    parser.add_argument('file', type=str,
            help='File to process.')
    parser.add_argument('-e', '--event', type=str,
            help='File to process.')
    parser.add_argument('--show-spans', action="store_true", default=False,
            help='Show spans when there is an overlap')
    parser.add_argument('--find-spans', action="store_true", default=False,
            help='Find overlapping spans')
    parser.add_argument('--hide-rows', action="store_true", default=False,
            help='Hide rows')
    return parser.parse_args(args)


def main(args):
    pl.Config().set_tbl_rows(200)\
               .set_fmt_str_lengths(100)\
               .set_tbl_cols(20)
    

    base_filter = [
        pl.col('EVENT TYPE') == 'stop',
        pl.col('TIMESTAMP') != '', # Filter out empty timestamps for non pre-processed trace files.
        pl.col('DURATION').is_not_null(),
    ]
    filter = []
    if args.event:
        filter.append(pl.col('MESSAGE') == args.event)
    
    all_progress_trace = (pl.scan_csv(args.file)
                .filter(base_filter)
                .with_columns(pl.col('TIMESTAMP').alias('END_TIME'))
                .with_columns([
                    pl.col('END_TIME').str.to_datetime("%Y-%m-%dT%H:%M:%S%.f"),
                    (pl.col('DURATION')*1_000_000).cast(pl.datatypes.Duration)
                ])
                .with_columns([(pl.col("END_TIME") - pl.col("DURATION")).alias("START_TIME")])
    )
 
    f_progress_trace = all_progress_trace.filter(filter) if filter else all_progress_trace
    
    # Rank spans per TRACE ID by DURATION (1 = longest, 2 = second-longest, ...)
    ranked = (f_progress_trace
        .select([
            'MESSAGE',
            'TRACE ID',
            'SPAN ID',
            'DURATION',
            'TRANSACTION ID',
            'END_TIME'
        ])
        .with_columns(
            pl.col('DURATION')
              .rank(method='dense', descending=True)
              .over('TRACE ID')
              .alias('span_rank')
        )
    )

    # Longest span per TRACE ID
    longest = (ranked
        .filter(pl.col('span_rank') == 1)
        .rename({
            'MESSAGE': 'MESSAGE_1',
            'SPAN ID': 'SPAN ID_1',
            'DURATION': 'DURATION_1',
        })
    )

    second_ranked = (all_progress_trace
        .filter(pl.col('PARENT SPAN ID').is_not_null() & # Filter out root spans (multiple root spans can have the same trace id)
               ~(pl.col('MESSAGE').is_in(['applying transaction', 'restconf edit', 'validate', 'holding service write lock'])))
        .select([
            'MESSAGE',
            'START_TIME',
            'TRACE ID',
            'SPAN ID',
            'DURATION',
        ])
        .with_columns(
            pl.col('DURATION')
              .rank(method='dense', descending=True)
              .over('TRACE ID')
              .alias('span_rank')
        )
    )

    # Second-longest span per TRACE ID (different SPAN ID by construction)
    second_longest = (second_ranked
        .filter(pl.col('span_rank') == 1)
        .rename({
            'MESSAGE': 'MESSAGE_2',
            'SPAN ID': 'SPAN ID_2',
            'DURATION': 'DURATION_2',
        })
    )

    # Left join: keep all longest spans, attach matching second-longest spans
    data = (longest
        .join(second_longest, on='TRACE ID', how='left')
        .sort('DURATION_1', descending=True)
    )

    overlap = (
        data
        .join(
            all_progress_trace.filter([ # Filter to get all root spans
                pl.col("PARENT SPAN ID").is_null() |
                # restconf edit message have a parent span id set (but shouldn't).
                pl.col("MESSAGE").is_in(["restconf edit"])
            ]),
            how="cross",
            suffix="_right"
        )
        .filter(
            (pl.col("TRACE ID") != pl.col("TRACE ID_right")) &
            (
                (   # Find spans partly overlapping at the start
                    (pl.col("START_TIME") < pl.col("END_TIME_right")) &
                    (pl.col("END_TIME") >= pl.col("END_TIME_right"))
                ) |
                (   # Find spans completely contained
                    (pl.col("START_TIME") <= pl.col("START_TIME_right")) &
                    (pl.col("END_TIME") >= pl.col("END_TIME_right"))
                ) |
                (   # Find spans partly overlapping at the end
                    (pl.col("END_TIME") > pl.col("START_TIME_right")) &
                    (pl.col("END_TIME") <= pl.col("END_TIME_right"))
                )
            )
        )
        .group_by(["TRACE ID"])
        .agg(
            pl.len().alias("OVERLAP_COUNT")
        )
        .select(["TRACE ID", "OVERLAP_COUNT"])
    )

    data = (
        data
        .select([
            'MESSAGE_1',
            'TRACE ID',
            'SPAN ID_1',
            'DURATION_1',
            'MESSAGE_2',
            'SPAN ID_2',
            'DURATION_2',
        ])
        .join(
                overlap,
                how="left",
                on="TRACE ID",
        )
        .sort('DURATION_1', descending=True)
        .collect()
    )
    
    print(data)    


if __name__ == '__main__':
    main(parseArgs(sys.argv[1:]))
