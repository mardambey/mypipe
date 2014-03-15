# TODO:
- Do not use TableMapEventData in the MySQLMetadataManager (third party API leak into ours)
- Logging format should be controlled through config file, not code
- Add MySQLMetadataManagerSpec
- Expose the stdout producer through a some application that accepts the
	hostname, port, etc. and simply `tails` the binlog.
- Periodically refresh MySQL table metadata
- MySQLSpec should create the user table if it is not present.
- Add metrics into various parts of the pipeline.
- HBase producer
- HDFS producer (through HFile?)
- Add option to ignore current saved location and pick up from current master location.
- If we fall behind replication there is no way for us to detect that and
	recover if we are resuming from a position saved to disk.
- Close db connections on exit

# DONE:
- Split out MySQL table metadata fetcher from BinlogConumer
- Add a latency test / gauge used to tell us how long it takes for an event
	to enter the MySQL database and get acted upon by a producer (queued, not
	flushed).
- Make pipe name part of binlog position file name on disk (currently only
	producer name is in there)
- Error handling when a producer fails
- Move producers outside BinlogConsumer and into Pipe
- Split into multi-project build (api, samples, producers, mypipe)
- Create a test that will consume information and write it a file, then
	validates it. The test can issue the queries too.
- Create pipes, where we join a bunch of consumers to a producer (tracks its own binlog progress)
- If binlog position does not move there is no need to flush it to disk
- Create stdout producer
- Create ColumnMetadata type and make it part of Column and use it in Table
