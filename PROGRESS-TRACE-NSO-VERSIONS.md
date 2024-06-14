# NSO Progress Trace formats over NSO versions

The Progress Trace have been around in NSO for a long time and started by logging extended trace messages to the developer log containing the execution duration of individual tasks, useful for determining if things were executing as expected.

It was also added the possibility to export CSV (Comma Separated Values) to have a format more suitable for machines to read.

This table will try to give an overview how the format have evolved over time, as it can be a pain to create scripts that a compatible over the versions. It is not just the columns that have changed. Messages have been added, but also renamed and removed.

The goal is give better insight what NSO is doing and to be compatible with OpenTelemetry.

| NSO Version | Changes                                                                                                                                                                                                       | No. Columns | Comment                                                                                                  |
| ----------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ----------- | -------------------------------------------------------------------------------------------------------- |
| 4.7         | Columns: TIMESTAMP, TID, SESSION ID, CONTEXT, SUBSYSTEM, PHASE, SERVICE, SERVICE PHASE, COMMIT QUEUE ID, NODE, DEVICE, DEVICE PHASE, PACKAGE, DURATION, MESSAGE.                                              | 15          | This is just the version were this guide start, not that the columns were added. Weak concept of spans/traces. |
| 5.1         |                                                                                                                                                                                                               | 15          |                                                                                                          |
| 5.2         |                                                                                                                                                                                                               | 15          |                                                                                                          |
| 5.3         |                                                                                                                                                                                                               | 15          |                                                                                                          |
| 5.4         | New columns: EVENT TYPE,DATASTORE,ANNOTATION.<br>Renamed columns: TID -> TRANSACTION ID.                                                                                                                      | 19          | Introduction of spans (start/stop)                                                                       |
| 5.5         |                                                                                                                                                                                                               | 19          |                                                                                                          |
| 5.6         |                                                                                                                                                                                                               | 19          |                                                                                                          |
| 5.7         | New columns: TRACE ID                                                                                                                                                                                         | 19          | Introduced to "replace" commit-queue tag and make tracing in LSA possible.                               |
| 5.8         |                                                                                                                                                                                                               | 19          |                                                                                                          |
| 6.0         |                                                                                                                                                                                                               | 19          |                                                                                                          |
| 6.1         | New columns: SPAN ID, PARENT SPAN ID, ATTRIBUTE NAME, ATTRIBUTE VALUE, LINK TRACE ID, LINK SPAN ID.<br> Removed columns: PHASE, SERVICE, SERVICE PHASE, COMMIT QUEUE ID, NODE, DEVICE, DEVICE PHASE, PACKAGE. | 17          | Introduction of traces.                                                                                                         |
| 6.2         |                                                                                                                                                                                                               | 17          |                                                                                                          |



# Preprocessing
```
Raw 5.7 log --> Pre-process (Fix locks etc.) ──────────────────────────> Process/analyza etc.
                                              │                      │
                                              └──> Merge (CRS/RFS)───┘  
```

5.x: Create syntetic spans for 'holding transaction lock' and 'holding device lock'.
     There are no duration metrics for how long the CDB is locked.
     
6.x: Covert atribute names/values into separate columns (for easier filtering)
     Additional attributes are written as new lines with the static column empty and only attribute_name and value filled in (to save space).



# CDB lock

## 5.7, 5.8

'stop' + 'grabbing transaction lock' -> 'info', 'releasing transaction lock' or 'stop', 'apply transaction', 'stopped'


## 6.x

'start' + 'taking transaction lock' -> 'stop', 'taking transaction lock'
'start' + 'holding transaction lock' -> 'stop', 'holding transaction lock'



#Dry-run

## 5.7

CFS:
start,2024-05-03T13:05:37.734037,,84,1126,running,cli,f57a7f46-c451-4a94-9cc7-b924e3660487,,abort,,,,,,,,"abort",
info,2024-05-03T13:05:37.734079,,84,1126,running,cli,f57a7f46-c451-4a94-9cc7-b924e3660487,cdb,abort,,,,,,,,"abort",
info,2024-05-03T13:05:37.735934,,84,1126,running,cli,f57a7f46-c451-4a94-9cc7-b924e3660487,,,,,,,,,,"releasing transaction lock",
stop,2024-05-03T13:05:37.736096,0.002059,84,1126,running,cli,f57a7f46-c451-4a94-9cc7-b924e3660487,,abort,,,,,,,,"abort",
stop,2024-05-03T13:05:37.736130,0.128458,84,1126,running,cli,f57a7f46-c451-4a94-9cc7-b924e3660487,,,,,,,,,,"applying transaction","error"

RFS:
start,2024-05-03T13:05:37.727707,,148,512,running,netconf,f57a7f46-c451-4a94-9cc7-b924e3660487,,abort,,,,,,,,"abort",
info,2024-05-03T13:05:37.727736,,148,512,running,netconf,f57a7f46-c451-4a94-9cc7-b924e3660487,cdb,abort,,,,,,,,"abort",
info,2024-05-03T13:05:37.728753,,148,512,running,netconf,f57a7f46-c451-4a94-9cc7-b924e3660487,,,,,,,,,,"releasing transaction lock",
stop,2024-05-03T13:05:37.728974,0.001267,148,512,running,netconf,f57a7f46-c451-4a94-9cc7-b924e3660487,,abort,,,,,,,,"abort",
stop,2024-05-03T13:05:37.729009,0.027270,148,512,running,netconf,f57a7f46-c451-4a94-9cc7-b924e3660487,,,,,,,,,,"applying transaction","error"