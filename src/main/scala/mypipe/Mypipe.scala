package mypipe

import com.github.shyiko.mysql.binlog.BinaryLogClient
import com.github.shyiko.mysql.binlog.BinaryLogClient.{ LifecycleListener, EventListener }
import com.github.shyiko.mysql.binlog.event.Event
import com.typesafe.config.ConfigFactory

import scala.collection.JavaConverters._
import java.io.{ PrintWriter, File }

object Conf {

  val conf = ConfigFactory.load()
  val sources = conf.getStringList("mypipe.sources")
  val DATADIR = conf.getString("mypipe.datadir")

  def binlogStatusFile(hostname: String, port: Int): String = {
    s"$DATADIR/$hostname-$port.pos"
  }

  def binlogFilePos(hostname: String, port: Int): Option[BinlogFilePos] = {
    try {

      val statusFile = binlogStatusFile(hostname, port)
      val filePos = scala.io.Source.fromFile(statusFile).getLines().mkString.split(":")
      Some(BinlogFilePos(filePos(0), filePos(1).toLong))

    } catch {
      case e: Exception ⇒ {
        println(e.getMessage + ": " + e.getStackTraceString)
        None
      }
    }
  }

  def binlogFilePosSave(hostname: String, port: Int, filePos: BinlogFilePos) {
    val fileName = binlogStatusFile(hostname, port)
    val writer = new PrintWriter(new File(fileName))
    writer.write(s"${filePos.filename}:${filePos.pos}")
    writer.close()
  }
}

case class BinlogFilePos(filename: String, pos: Long)

object BinlogFilePos {
  val current = BinlogFilePos("", 0)
}

case class BinlogConsumer(hostname: String, port: Int, username: String, password: String, binlogFileAndPos: BinlogFilePos) {

  val client = new BinaryLogClient(hostname, port, username, password);
  client.registerEventListener(new EventListener() {

    override def onEvent(event: Event) {
      println(event.toString)
    }
  })

  client.registerLifecycleListener(new LifecycleListener {
    override def onDisconnect(client: BinaryLogClient): Unit = {
      Conf.binlogFilePosSave(hostname, port, BinlogFilePos(client.getBinlogFilename, client.getBinlogPosition))
    }

    override def onEventDeserializationFailure(client: BinaryLogClient, ex: Exception) {}
    override def onConnect(client: BinaryLogClient) {}
    override def onCommunicationFailure(client: BinaryLogClient, ex: Exception) {}
  })

  def connect() {
    client.connect()
  }

  def disconnect() {
    client.disconnect()
  }
}

object Mypipe extends App {

  val consumers = Conf.sources.asScala.map(
    source ⇒ {
      val params = source.split(":")
      val filePos = Conf.binlogFilePos(params(0), params(1).toInt).getOrElse(BinlogFilePos.current)
      BinlogConsumer(params(0), params(1).toInt, params(2), params(3), filePos)
    })

  consumers.foreach(c ⇒ c.connect())

  sys.addShutdownHook({
    consumers.foreach(c ⇒ c.disconnect())
  })

  Thread.sleep(30000)

}
