package mypipe

import com.typesafe.config.ConfigFactory
import java.io.{ PrintWriter, File }
import mypipe.mysql.BinlogFilePos

object Conf {

  val conf = ConfigFactory.load()
  val sources = conf.getStringList("mypipe.sources")
  val DATADIR = conf.getString("mypipe.data-dir")
  val LOGDIR = conf.getString("mypipe.log-dir")
  val SHUTDOWN_FLUSH_WAIT_SECS = conf.getInt("mypipe.shutdown-wait-time-seconds")
  val GROUP_EVENTS_BY_TX = conf.getBoolean("mypipe.group-events-by-tx")
  val FLUSH_INTERVAL_SECS = Conf.conf.getInt("mypipe.flush-interval-seconds")

  try {
    new File(DATADIR).mkdirs()
    new File(LOGDIR).mkdirs()
  } catch {
    case e: Exception ⇒ println(s"Error while creating data and log dir ${DATADIR}, ${LOGDIR}: ${e.getMessage}")
  }

  def binlogStatusFile(hostname: String, port: Int): String = {
    s"$DATADIR/$hostname-$port.pos"
  }

  def binlogFilePos(hostname: String, port: Int): Option[BinlogFilePos] = {
    try {

      val statusFile = binlogStatusFile(hostname, port)
      val filePos = scala.io.Source.fromFile(statusFile).getLines().mkString.split(":")
      Some(BinlogFilePos(filePos(0), filePos(1).toLong))

    } catch {
      case e: Exception ⇒ None
    }
  }

  def binlogFilePosSave(hostname: String, port: Int, filePos: BinlogFilePos) {
    Log.info(s"Saving binlog position for $hostname:$port => $filePos")
    val fileName = binlogStatusFile(hostname, port)
    val file = new File(fileName)
    val writer = new PrintWriter(file)
    writer.write(s"${filePos.filename}:${filePos.pos}")
    writer.close()
  }
}