package mypipe

import mypipe.producer.cassandra.CassandraProducer
import mypipe.mysql.{ BinlogConsumer, BinlogFilePos, HostPortUserPass }
import scala.collection.JavaConverters._

object Mypipe extends App {

  val producer = CassandraProducer()

  val consumers = Conf.sources.asScala.map(
    source ⇒ {
      val params = HostPortUserPass(source)
      val filePos = Conf.binlogFilePos(params.host, params.port).getOrElse(BinlogFilePos.current)
      val consumer = BinlogConsumer(params.host, params.port, params.user, params.password, filePos)
      consumer.registerProducer(producer)
      consumer
    })

  sys.addShutdownHook({
    Log.info("Shutting down...")
    consumers.foreach(c ⇒ c.disconnect())
  })

  val threads = consumers.map(c ⇒ new Thread() { override def run() { c.connect() } })
  threads.foreach(_.start())
  threads.foreach(_.join())
}
