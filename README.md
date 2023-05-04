# Progress Trace Viewer

## Overview

This repository contains a number of very simplistic tools to analyze the
content of a progress trace csv file.

Works with NSO 5.x and 6.0.
Not tested with NSO 6.1.

## Dependecies

The scripts in this repository have dependencies to e.g. rich and pandas.
You can use the file 'requirements.txt' to install them.

```
pip3 install -r requirements.txt
```

## Tools

### ncs_progress_trace_viewer

The viewer reads a progress trace and uses the Python module 'rich' to visualize
the spans.

```
❯ ./ncs_progress_trace_viewer.py -h
usage: ncs_progress_trace_viewer.py [-h] [-f] [-o] [--setup] [file]

positional arguments:
  file          File to process.

optional arguments:
  -h, --help       show this help message and exit
  -f, --follow     Follow file and graph as traces come.
  -o               Graph operational transactions.
  --setup          Config progress trace in NSO.
  --filter FILTER  Read events to filter from file.
  --tid TID        Filter on transaction id.
  --write WRITE    Write the progress trace events to file.
  --interactive    Skip view updates and sleeps during parsing.
```

![Screenshot](images/progress_trace.png)

### list_tids

List transactions id:s in progress trace file.

```
❯ ./list_tids.py -h
usage: list_tids.py [-h] [--event EVENT] [-b BEGIN] [-e END] file

positional arguments:
  file                  File to process.

optional arguments:
  -h, --help            show this help message and exit
  --event EVENT         File to process.
  -b BEGIN, --begin BEGIN
                        Start timestamp
  -e END, --end END     End timestamp
```

## list_events

List event types in progress trace.

```
❯ ./list_events.py -h
usage: list_events.py [-h] [-n] file

positional arguments:
  file          File to process.

optional arguments:
  -h, --help    show this help message and exit
  -n, --negate  Negate/comment events.
```

### calc_events_stats

Calculate statistical analysis of a progress trace.

```
❯ ./calc_events_stats.py -h
usage: calc_events_stats.py [-h] file

positional arguments:
  file        File to process.

optional arguments:
  -h, --help  show this help message and exit
```

### list_events_duration

List events duration in descending order (duration, device name, tid).

```
❯ ./list_events_duration.py -h
usage: list_events_duration.py [-h] [--event EVENT] [-b BEGIN] [-e END] file

positional arguments:
  file                  File to process.

optional arguments:
  -h, --help            show this help message and exit
  --event EVENT         File to process.
  -b BEGIN, --begin BEGIN
                        Start timestamp
  -e END, --end END     End timestamp
```

### show_overlap
```
❯ ./show_overlap.py -h
usage: show_overlap.py [-h] [--show-spans] [--find-spans] [--hide-rows] file event

positional arguments:
  file          File to process.
  event         File to process.

optional arguments:
  -h, --help    show this help message and exit
  --show-spans  Show spans when there is an overlap
  --find-spans  Find overlapping spans
  --hide-rows   Hide rows
```

### summarize_events

Make a summary analysis of a progress trace.

```
❯ ./summarize_events.py -h
usage: summarize_events.py [-h] [--event EVENT] [-5] [--old] file

positional arguments:
  file           File to process.

optional arguments:
  -h, --help     show this help message and exit
  --event EVENT  File to process.
  -5             Handle as NSO 5.x compatible progress trace.
  --old          Use event 'apply transaction' as lock event.
```

```
❯ ./summarize_events.py -5 testdata/nso5.8-devices-sync-from-bad.csv
===== testdata/nso5.8-devices-sync-from-bad.csv =====

Number of actions:              100
Total time:                     138.5 s
Total number of locks:          20
Total time inside locks:        2.2 s
Percent spent within lock:      2 %

Time to first lock:             135.822929 s

Total time between locks:       0.469249 s
Mean time between locks:        0.024697 s
Stddev time between locks:      0.064
Min time between locks:         0.005433 s
Max time between locks:         0.288114 s
```

## Test data

Progress trace test data from various devices sync-from actions using two
different versions of the cisco-ios-cli NED resides in the testdata directory.

677 indicates that the version 6.77 of the cisco-ios-cli NED was used and
692 indicates 6.92.

The 6.77 version of the NED contains a bug that is triggered by a specific
combination of configuration in the device.
The bug is fixed in version 6.88 and newer of the NED.

| File name | Description |
|-----------|-------------|
| nso5.8-devices-sync-from-677-good.csv | A full devices sync-from of 100 devices, containing no configuration triggering the bug. |
| nso5.8-devices-sync-from-677-bad.csv | A full devices sync-from of 100 devices, containing the configuration triggering the bug. This will cause sync-from towards some devices to timeout. |
| nso5.8-devices-sync-from-677-bad-longer-timeout.csv | A full devices sync-from of 100 devices, containing the configuration triggering the bug. The read-timeout have been increased to mitigate the timeouts. |
| nso5.8-devices-sync-from-677-bad-sequential.csv | A "full" devices device * sync-from of 100 devices, containing the configuration triggering the bug. The sequential execution will not trigger the bug. |
| nso5.8-devices-sync-from-677-one-device.csv | Sync-from of one device will not trigger the bug. |
| nso5.8-devices-sync-from-677.csv | A full devices sync-from of 100 devices, containing the configuration triggering the bug. |
| nso5.8-devices-sync-from-692-one-device.csv | Sync-from of one device with a newer version of the NED will not triggr the bug. |
| nso5.8-devices-sync-from-692.csv | A full devices sync-from of 100 devices with a new version of the NED will not trigger the bug. |
