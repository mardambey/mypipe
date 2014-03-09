mypipe
===

mypipe latches onto a MySQL server with binary log replication enabled and
allows for the creation of pipes that can consume the replication strem and
act on the data.

Producers
---

Producers receive MySQL binary log events and act on them. They can funnel
down to another data store, send them to some service, or just print them
out to the screen in the case of the stdout producer.

Producers can also make use of mappings. Mappings will map the data from one
form to another (providing minimal data transformations) before the producer 
flushes the data out.

Pipes
---

Pipes tie one or more MySQL binary log consumers to a producer. Pipes can be
used to create a system of fan-in data flow from several MySQL servers to
other data sources such as Hadoop, Cassandra, Kafka, or other MySQL servers.
They can also be used to update or flush caches. 
