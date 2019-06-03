# spark-etl-demo-extract-job-mix
- extract and parse daily csv files with mix schema from single source directory (see example csv files from directory: data_raw)
- Handle the changes of csv schema over time => new columns & columns order shifting
- Handle missing data issues and gracefully convert to `null` values when => missing values, missing columns, or data values can't be cast to predefined schema
- Output to multiple daily partitioned folders with `dt=date` prefix in `parquet` format
- Augment the data rows with source file date

# Requirements
- jdk: 8
- scala: 2.11.12
- sbt: 1.2.8

# Test & Assembly Spark Application jar
Method A - Use Local Installed SBT
```
sbt clean test
sbt clean test assembly
```
Method B (Alternative) - Use Pre-configured SBT Docker build image, eg: [qiqi/sbt:sbt1.2.8_jdk8u212](https://hub.docker.com/r/qiqi/sbt)
```
docker run -it -v $PWD:/repo -w /repo --rm qiqi/sbt:sbt1.2.8_jdk8u212 sbt clean test
docker run -it -v $PWD:/repo -w /repo --rm qiqi/sbt:sbt1.2.8_jdk8u212 sbt clean test assembly
```
Packaged jar location:
```
./target/scala-2.11/spark-etl-demo-extract-job-mix-assembly-0.1.jar
```

# Expected Command Line Arguments
- --startDate => start date to process
    - ex: `2018-07-01`
- --daysToRun => number of days to process
    - ex: `3`
- --srcPath => source parent directory path: raw csv files
    - ex: `./data_raw`
- --dstPath => destination parent directory path: output results
    - ex: `./data_extract`

# Local Run Example with Spark in Docker
- Run with spark-submit in local master mode
    - --master local[*]
- Spark Docker repo: [qiqi/spark:spark2.4.3_hadoop2.7](https://cloud.docker.com/u/qiqi/repository/docker/qiqi/spark)
    - https://github.com/qi-qi/docker-spark
```
docker run --rm -v $PWD:/work -w /work qiqi/spark:spark2.4.3_hadoop2.7 \
spark-submit \
--master local[*] \
--class cloud.qiqi.Main \
./target/scala-2.11/spark-etl-demo-extract-job-mix-assembly-0.1.jar \
--startDate 2018-07-01 \
--daysToRun 3 \
--srcPath ./data_raw \
--dstPath ./data_extract
```
Results will be generated in ./data_extract folder. Ex:
```
Qi-MBP13-Acast:spark-etl-demo-extract-job-mix qi$ ll
total 16
-rw-r--r--    1 qi  staff  1978 May 26 21:54 README.md
-rw-r--r--    1 qi  staff   341 May 26 21:18 build.sbt
drwxr-xr-x    7 qi  staff   224 May 26 21:50 data_extract   <= output result
drwxr-xr-x  167 qi  staff  5344 May 26 17:32 data_raw       <= raw csv source files
drwxr-xr-x    6 qi  staff   192 May 26 19:57 project
drwxr-xr-x    4 qi  staff   128 May 26 15:30 src
drwxr-xr-x    5 qi  staff   160 May 26 21:34 target
```

# Input Source
- Source file sample1 => Ex: 2018-07-01.csv
```
temperature,skipped_beat,at_risk
LOW,,1
HIGH,1.0,1
HIGH,2.0,1
LOW,1.0,1
HIGH,0.0,0
HIGH,0.0,0
LOW,1.0,0
LOW,0.0,0
LOW,1.0,1
MEDIUM,2.0,1
```
- Source file sample2 => Ex: 2018-11-01.csv
```
temperature,at_risk,skipped_beat,price
MEDIUM,0,1.0,7229.701308568062
HIGH,0,4.0,5311.273197841647
HIGH,1,0.0,122980.54818748399
HIGH,0,1.0,3294.1124428217627
HIGH,1,,30773.396784116958
LOW,0,1.0,26507.74788280587
MEDIUM,0,0.0,13063.470347391793
LOW,0,1.0,61326.72292822154
LOW,0,,57640.9394153083
LOW,0,0.0,4739.52631340721
```

- Source file path pattern
```
./data_raw/
├── 2018-07-01.csv
├── 2018-07-02.csv
├── 2018-07-03.csv
├── 2018-07-04.csv
├── 2018-07-05.csv
...
├── 2018-12-01.csv
├── 2018-12-02.csv
├── 2018-12-03.csv
├── 2018-12-04.csv
├── 2018-12-05.csv
├── 2018-12-06.csv
...
```

# Output Result
- Output result schema
```
schema
 |-- event_date: date (nullable = false)
 |-- temperature: string (nullable = true)
 |-- at_risk: integer (nullable = true)
 |-- skipped_beat: float (nullable = true)
 |-- price: double (nullable = true)
```
- Output result sample1 => 2018-07-01
```
+----------+-----------+-------+------------+-----+
|event_date|temperature|at_risk|skipped_beat|price|
+----------+-----------+-------+------------+-----+
|2018-07-01|LOW        |1      |null        |null |
|2018-07-01|HIGH       |1      |1.0         |null |
|2018-07-01|HIGH       |1      |2.0         |null |
|2018-07-01|LOW        |1      |1.0         |null |
|2018-07-01|HIGH       |0      |0.0         |null |
|2018-07-01|HIGH       |0      |0.0         |null |
|2018-07-01|LOW        |0      |1.0         |null |
|2018-07-01|LOW        |0      |0.0         |null |
|2018-07-01|LOW        |1      |1.0         |null |
|2018-07-01|MEDIUM     |1      |2.0         |null |
+----------+-----------+-------+------------+-----+
```
- Output result sample2 => 2018-11-01
```
+----------+-----------+-------+------------+------------------+
|event_date|temperature|at_risk|skipped_beat|price             |
+----------+-----------+-------+------------+------------------+
|2018-11-01|MEDIUM     |0      |1.0         |7229.701308568062 |
|2018-11-01|HIGH       |0      |4.0         |5311.273197841647 |
|2018-11-01|HIGH       |1      |0.0         |122980.54818748399|
|2018-11-01|HIGH       |0      |1.0         |3294.1124428217627|
|2018-11-01|HIGH       |1      |null        |30773.396784116958|
|2018-11-01|LOW        |0      |1.0         |26507.74788280587 |
|2018-11-01|MEDIUM     |0      |0.0         |13063.470347391793|
|2018-11-01|LOW        |0      |1.0         |61326.72292822154 |
|2018-11-01|LOW        |0      |null        |57640.9394153083  |
|2018-11-01|LOW        |0      |0.0         |4739.52631340721  |
+----------+-----------+-------+------------+------------------+
```
- Output result path pattern
```
./data_extract/
├── dt=2018-07-01/
│   └── part-00000-c12a4ec1-1a23-497f-9085-0f6ebc98a37c.c000.snappy.parquet
├── dt=2018-07-02/
│   └── part-00000-fc598213-92e9-45e4-897c-7454cde24a3d.c000.snappy.parquet
├── dt=2018-07-03/
│   └── part-00000-fe3153c4-8b52-477d-a5b1-81c34d488701.c000.snappy.parquet
├── dt=2018-07-04/
│   └── part-00000-4b835d5c-731b-4c3d-bfd4-71d611a79e0e.c000.snappy.parquet
├── dt=2018-07-05/
│   └── part-00000-6aa954e2-9812-476b-b8a0-8d4bee062f4b.c000.snappy.parquet
...
├── dt=2018-12-01/
│   └── part-00000-54bdf475-4caa-44a8-ad73-d6cef45120cc.c000.snappy.parquet
├── dt=2018-12-02/
│   └── part-00000-d8fd40cc-b00a-4bad-bf42-3ea0bdb62605.c000.snappy.parquet
├── dt=2018-12-03/
│   └── part-00000-937b5cf4-dee6-464e-b089-db9c5e6c1d2d.c000.snappy.parquet
├── dt=2018-12-04/
│   └── part-00000-f48998b5-8d7f-4ad5-9717-d97bd8acd762.c000.snappy.parquet
├── dt=2018-12-05/
│   └── part-00000-259af112-b87d-449c-8cf7-dda27b5a66cc.c000.snappy.parquet
├── dt=2018-12-06/
│   └── part-00000-ff3adf21-76b3-4688-aafe-a5e577c462b0.c000.snappy.parquet
...
```
# Comments:
- change the raw data delivery format and source directory:
    - ideally, the daily delivery source file should be put into separate directories with date prefix
    - the format should be changed from raw csv to parquet/orc/avro, or multi-line json in gz
