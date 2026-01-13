# data model

## concepts
What is a snapshot?

What is reference data?

What is the grain?

What is mutable vs immutable?

## reference data
Line–stop mapping

It is low-frequency, snapshot-based (not bronze), replace-latest semantics per day. Multiple runs within a day overwrite the same partition.

## timetable data

Grain definition

One row = one scheduled arrival per stop per line per service day

Why knownJourneys is exploded

Why service days are normalized

Why arrival time is stored twice (time + minutes)

## future fact tables
...




# schemas 
Each schema file should contain:

- Table purpose

- Grain

- Columns table

- Constraints / assumptions

- Example rows


## line_stop_mapping

## stop_timetable_snapshot

**Grain**  
One row per scheduled arrival time per stop per line per service day.

| column | type | description |
|------|------|-------------|
| snapshot_date | date | ingestion date |
| line_id | string | TfL line id |
| stop_id | string | NaPTAN id |
| stop_sequence | int | route topology |
| direction | string | inbound / outbound |
| service_day | string | normalized weekday |
| arrival_time | time | HH:MM |
| arrival_minutes | int | minutes since midnight |
| interval_id | string | source traceability |
| source | string | lineage |


Not all line–stop pairs have scheduled timetables. The ingestion layer treats HTTP 400/404 responses as valid ‘no-service’ states and records only existing schedules.

## bronze

## silver

## gold


