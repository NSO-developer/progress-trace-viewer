#!/usr/bin/env python3

import argparse
from datetime import datetime
import sys

import pandas


def parseArgs(args):
    parser = argparse.ArgumentParser()
    parser.add_argument('file', type=str,
            help='File to process.')
    parser.add_argument('--event', type=str, default='sync-from',
            help='File to process.')
    parser.add_argument('-5', action='store_true', default=False, dest='nso5',
            help='Handle as NSO 5.x compatible progress trace.')
    parser.add_argument('--old', action='store_true', default=False,
            help='Use event \'apply transaction\' as lock event.')
    return parser.parse_args(args)


def sum_duration_between_locks_5(htl_events):
    # Collect duration between locks and calculate statistics
    bt_ts = []
    bt_dur = []
    start = None
    end = None
    overlap_cnt = 0
    overlap_tids = []
    overlap_start = None
    overlag_stop = None
    for i, ev in htl_events.sort_values('TIMESTAMP').iterrows():
        ts = ev['TIMESTAMP']
        tid = ev['TRANSACTION ID']
        if ev['EVENT TYPE'] == 'stop':
            start = ts
            overlap_tids.append(tid)
            if len(overlap_tids) == 1:
                if end:
                    bt_ts.append(ts)
                    bt_dur.append((ts-end).total_seconds())
                    end = None
            else:
                print("Overlapping events, count as 0 sec gap.", overlap_tids)
        elif ev['EVENT TYPE'] == 'info' and start is not None:
            if tid in overlap_tids:
                overlap_tids.remove(tid)
                if len(overlap_tids) == 0:
                    end = ts
            htl_events.at[i, 'DURATION'] = (ts-start).total_seconds()
        p_ts = ts
        p_tid = ev['TRANSACTION ID']
    return pandas.DataFrame(index=bt_ts, data={'DURATION': bt_dur})


def sum_duration_between_locks_6(htl_events):
    # Collect duration between locks and calculate statistics
    bt_ts = []
    bt_dur = []
    end = None
    overlap_cnt = 0
    overlap_tids = []
    overlap_start = None
    overlag_stop = None
    for _, ev in htl_events.sort_values('TIMESTAMP').iterrows():
        ts = ev['TIMESTAMP']
        tid = ev['TRANSACTION ID']
        if ev['EVENT TYPE'] == 'start':
            overlap_tids.append(tid)
            if len(overlap_tids) == 1:
                if end:
                    bt_ts.append(ts)
                    bt_dur.append((ts-end).total_seconds())
                    end = None
            else:
                print("Overlapping events, count as 0 sec gap.", overlap_tids)
        elif ev['EVENT TYPE'] == 'stop':
            if tid in overlap_tids:
                overlap_tids.remove(tid)
                if len(overlap_tids) == 0:
                    end = ts
        p_ts = ts
        p_tid = ev['TRANSACTION ID']

    return pandas.DataFrame(index=bt_ts, data={'DURATION': bt_dur})


def main(args):
    print("=====", args.file, "=====")
    progress_trace = pandas.read_csv(args.file,
                             low_memory=False,
                             converters={'TIMESTAMP': datetime.fromisoformat})

    have_trace_id = 'TRACE ID' in progress_trace.keys()

    if args.nso5:
        if args.old:
            lock_et = 'start'
            lock_event = 'applying transaction'
        else:
            lock_et = 'stop'
            lock_event = 'grabbing transaction lock'
    else:
        lock_et = 'start'
        lock_event = 'holding transaction lock'

    d = progress_trace[progress_trace['DATASTORE'] == 'running']

    if args.nso5 and not args.old:
        htl_events = d[
            ((d['EVENT TYPE'] == "stop") &
             (d['MESSAGE'] == "grabbing transaction lock")) |
            ((d['EVENT TYPE'] == "info") &
             (d['MESSAGE'] == "releasing transaction lock"))
             ]
        bt_locks = sum_duration_between_locks_5(htl_events)
        htl_sum = htl_events[htl_events['EVENT TYPE'] == 'info']['DURATION'].sum()
    else:
        htl_events = d[d['MESSAGE'] == lock_event]
        bt_locks = sum_duration_between_locks_6(htl_events)
        htl_sum = htl_events[htl_events['EVENT TYPE'] == 'stop']['DURATION'].sum()

    bt_locks_sum = bt_locks["DURATION"].sum()
    bt_locks_std = bt_locks["DURATION"].std()
    bt_locks_mean = bt_locks["DURATION"].mean()
    bt_locks_min = bt_locks["DURATION"].min()
    bt_locks_max = bt_locks["DURATION"].max()

    t_events = d[d['MESSAGE'] == args.event]
    if len(t_events) == 0:
        print(f"ERROR: No events of type '{args.event}' found.")
        sys.exit(2)
    t_cnt = t_events[t_events['EVENT TYPE'] == 'start']['EVENT TYPE'].count()
    first_ts = d.iloc[0]['TIMESTAMP']
    last_ts = t_events[t_events['EVENT TYPE'] == 'stop'].iloc[-1]['TIMESTAMP']
    delta = (last_ts-first_ts).total_seconds()
    htl_starts = htl_events[htl_events['EVENT TYPE'] == lock_et]
    htl_cnt = htl_starts['MESSAGE'].count()
    if len(htl_starts):
        first_lock = htl_starts.iloc[0]['TIMESTAMP']
        delta_lock = (first_lock-first_ts).total_seconds()
    else:
        delta_lock = float('NaN')

    print()
    print(f"Number of actions:              {t_cnt}")
    print(f"Total time:                     {delta:.1f} s")
    print(f"Total number of locks:          {htl_cnt}")
    print(f"Total time inside locks:        {htl_sum:.1f} s")
    print(f"Percent spent within lock:      "+
          f"{(htl_sum/delta)*100:.0f} %")
    print()
    print(f"Time to first lock:             {delta_lock:.6f} s")
    print()
    print(f"Total time between locks:       {bt_locks_sum:.6f} s")
    print(f"Mean time between locks:        {bt_locks_mean:.6f} s")
    print(f"Stddev time between locks:      {bt_locks_std:.3f}")
    print(f"Min time between locks:         {bt_locks_min:.6f} s")
    print(f"Max time between locks:         {bt_locks_max:.6f} s")


if __name__ == '__main__':
    main(parseArgs(sys.argv[1:]))
