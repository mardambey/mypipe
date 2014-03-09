# mypipe

mypipe latches onto a MySQL server with binary log replication enabled and
allows for the creation of pipes that can consume the replication stream and
act on the data.

mypipe tries to provide enough information that usually is not part of the
MySQL binary log stream so that the data is meaningful. mypipe requires a
row based binary log format and provides `Insert`, `Update`, and `Delete`
mutations representing changed rows. Each change is related back to it's
table and the API provides metadata like column types, primary key
information (composite, key order), and other such useful information.

## Producers

Producers receive MySQL binary log events and act on them. They can funnel
down to another data store, send them to some service, or just print them
out to the screen in the case of the stdout producer.

Producers can also make use of mappings. Mappings will map the data from one
form to another (providing minimal data transformations) before the producer 
flushes the data out.

## Pipes

Pipes tie one or more MySQL binary log consumers to a producer. Pipes can be
used to create a system of fan-in data flow from several MySQL servers to
other data sources such as Hadoop, Cassandra, Kafka, or other MySQL servers.
They can also be used to update or flush caches.
