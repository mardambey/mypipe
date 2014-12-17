# TODO:
- Make binlog position saving (currently to file) modular
- Create a tool that facilitates altering a MySQL table and pushes the
	corresponding Avro schema to the repository.
- If the MySQL tables have less fields then the Avro schema we get an NPE,
	only set fields that are present, and attempt to set some default values
	on fields that we do not get from the database
- Add ability to create and register avro schemas dynamically as tables change
- Expose the stdout producer through a some application that accepts the
	hostname, port, etc. and simply `tails` the binlog.
- Add metrics into various parts of the pipeline.
- HBase producer
- HDFS producer (through HFile?)
- Add option to ignore current saved location and pick up from current master location.
- If we fall behind replication there is no way for us to detect that and
	recover if we are resuming from a position saved to disk.
- Close db connections on exit

# DONE:
- Use single topic per table for the Kafka generic producer
- Logging format should be controlled through config file, not code
- Create Kafka consumer
- Periodically refresh MySQL table metadata -> instead, handle ALTER events.
	This is taken care of since we cache tables by MySQL table ID which will
	chanage when the tables meta data changes.
- Split off Cassandra producer into mypipe-cassandra
- Add Kafka 0.8 producer
- Do not use TableMapEventData in the MySQLMetadataManager (third party API leak into ours)
- MySQLSpec should create the user table if it is not present.
- Add MySQLMetadataManagerSpec
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
