package mypipe

import com.typesafe.config.ConfigFactory
import scala.collection.JavaConverters._
import java.io.{ PrintWriter, File }
import mypipe.mysql.BinlogFilePos
import mypipe.api.Log

object Conf {

  val conf = ConfigFactory.load()

  val DATADIR = conf.getString("mypipe.data-dir")
  val LOGDIR = conf.getString("mypipe.log-dir")

  val SHUTDOWN_FLUSH_WAIT_SECS = conf.getInt("mypipe.shutdown-wait-time-seconds")
  val GROUP_EVENTS_BY_TX = conf.getBoolean("mypipe.group-events-by-tx")
  val FLUSH_INTERVAL_SECS = Conf.conf.getInt("mypipe.flush-interval-seconds")

  val CONSUMERS = conf.getObject("mypipe.consumers").asScala
  val PRODUCERS = Conf.conf.getObject("mypipe.producers").asScala
  val PIPES = Conf.conf.getObject("mypipe.pipes").asScala

  val MYSQL_SERVER_ID_PREFIX = Conf.conf.getInt("mypipe.mysql-server-id-prefix")

  private val lastBinlogFilePos = scala.collection.mutable.HashMap[String, BinlogFilePos]()

  try {
    new File(DATADIR).mkdirs()
    new File(LOGDIR).mkdirs()
  } catch {
    case e: Exception ⇒ println(s"Error while creating data and log dir ${DATADIR}, ${LOGDIR}: ${e.getMessage}")
  }

  def binlogStatusFile(hostname: String, port: Int, pipe: String): String = {
    s"$DATADIR/$pipe-$hostname-$port.pos"
  }

  def binlogFilePos(hostname: String, port: Int, pipe: String): Option[BinlogFilePos] = {
    try {

      val statusFile = binlogStatusFile(hostname, port, pipe)
      val filePos = scala.io.Source.fromFile(statusFile).getLines().mkString.split(":")
      Some(BinlogFilePos(filePos(0), filePos(1).toLong))

    } catch {
      case e: Exception ⇒ None
    }
  }

  def binlogFilePosSave(hostname: String, port: Int, filePos: BinlogFilePos, pipe: String) {

    val key = binlogStatusFile(hostname, port, pipe)

    if (!lastBinlogFilePos.getOrElse(key, "").equals(filePos)) {

      val fileName = binlogStatusFile(hostname, port, pipe)
      val file = new File(fileName)
      val writer = new PrintWriter(file)

      Log.info(s"Saving binlog position for pipe $pipe/$hostname:$port -> $filePos")
      writer.write(s"${filePos.filename}:${filePos.pos}")
      writer.close()

      lastBinlogFilePos(key) = filePos
    }
  }
}