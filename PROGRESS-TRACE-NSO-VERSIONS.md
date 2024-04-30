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



# CDB lock

## 5.7

'stop' + 'grabbing transaction lock' -> 'info', 'releasing transaction lock'


## 6.x

'start' + 'grabbing transaction lock' -> 'info', 'grabbing transaction lock'