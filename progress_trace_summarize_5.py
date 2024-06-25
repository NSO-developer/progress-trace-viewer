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


def sum_duration_between_locks(htl_events):
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
                pass
                #print("Overlapping events, count as 0 sec gap.", overlap_tids)
        elif ev['EVENT TYPE'] == 'stop':
            if tid in overlap_tids:
                overlap_tids.remove(tid)
                if len(overlap_tids) == 0:
                    end = ts
        p_ts = ts
        p_tid = ev['TRANSACTION ID']

    return pandas.DataFrame(index=bt_ts, data={'DURATION': bt_dur})


def main(args):
    progress_trace = pandas.read_csv(args.file,
                             low_memory=False,
                             converters={'TIMESTAMP': datetime.fromisoformat})

    #d = progress_trace[progress_trace['DATASTORE'] == 'running']
    d = progress_trace
    
    first_ts = d.iloc[0]['TIMESTAMP']
    last_ts = d.iloc[-1]['TIMESTAMP']

    htl_events = d[d['MESSAGE'] == 'holding transaction lock']
    bt_locks = sum_duration_between_locks(htl_events)
    bt_locks_sum = bt_locks["DURATION"].sum()
    bt_locks_std = bt_locks["DURATION"].std()
    bt_locks_mean = bt_locks["DURATION"].mean()
    bt_locks_min = bt_locks["DURATION"].min()
    bt_locks_max = bt_locks["DURATION"].max()

    htl_start_events = htl_events[htl_events['EVENT TYPE'] == 'start']
    htl_stop_events = htl_events[htl_events['EVENT TYPE'] == 'stop'] 
    htl_cnt = htl_start_events['EVENT TYPE'].count()
    htl_first_ts = htl_start_events.iloc[0]['TIMESTAMP']
    htl_last_ts = htl_stop_events.iloc[-1]['TIMESTAMP']
    
    
    time_to_first_lock = (htl_first_ts-first_ts).total_seconds()
    time_after_last_lock = (last_ts-htl_last_ts).total_seconds()

#    htl_delta = (htl_last_ts-htl_first_ts).total_seconds()
    htl_total_time = (last_ts-first_ts).total_seconds()
    htl_lock_time = htl_stop_events['DURATION'].sum()

    if htl_cnt:
        first_lock = htl_start_events.iloc[0]['TIMESTAMP']
        delta_lock = (first_lock-htl_first_ts).total_seconds()
    else:
        delta_lock = float('NaN')

    print(f"Time to first lock:             {time_to_first_lock}")
    print(f"Time after last lock:           {time_after_last_lock}")
    print()
    print(f"Total time:              {htl_total_time:>12.1f} s")
    print(f"Total number of locks: {htl_cnt:>12}")
    print(f"Total time inside locks: {htl_lock_time:>12.1f} s")
    print(f"Percent spent within lock:"+
          f"{(htl_lock_time/htl_total_time)*100:>9.0f} %")
    print()
    print(f"Total time between locks:     {bt_locks_sum:>12.6f} s")
    print(f"Mean time between locks:      {bt_locks_mean:>12.6f} s")
    print(f"Stddev time between locks:    {bt_locks_std:>12.6f}")
    print(f"Min time between locks:       {bt_locks_min:>12.6f} s")
    print(f"Max time between locks:       {bt_locks_max:>12.6f} s")


if __name__ == '__main__':
    main(parseArgs(sys.argv[1:]))
