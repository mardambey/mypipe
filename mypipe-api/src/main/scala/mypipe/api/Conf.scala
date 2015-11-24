package mypipe.api

import java.io.{ File, PrintWriter }

import com.typesafe.config.{ Config, ConfigFactory }
import mypipe.mysql.BinaryLogFilePosition
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._

object Conf {

  implicit class RichConfig(val underlying: Config) extends AnyVal {
    def getOptionalString(path: String): Option[String] = if (underlying.hasPath(path)) {
      Some(underlying.getString(path))
    } else {
      None
    }

    def getOptionalNoneEmptyString(path: String): Option[String] = if (underlying.hasPath(path)) {
      underlying.getString(path) match {
        case s if s.nonEmpty ⇒ Some(s)
        case _               ⇒ None
      }
    } else {
      None
    }
  }

  val log = LoggerFactory.getLogger(getClass)
  val conf = ConfigFactory.load()

  val DATADIR = conf.getString("mypipe.data-dir")
  val LOGDIR = conf.getString("mypipe.log-dir")

  val SHUTDOWN_FLUSH_WAIT_SECS = conf.getInt("mypipe.shutdown-wait-time-seconds")
  val FLUSH_INTERVAL_SECS = conf.getInt("mypipe.flush-interval-seconds")

  val GROUP_EVENTS_BY_TX = conf.getBoolean("mypipe.group-events-by-tx")
  val GROUP_MUTATIONS_BY_TX = conf.getBoolean("mypipe.group-mutations-by-tx")

  val QUIT_ON_LISTENER_FAILURE = conf.getBoolean("mypipe.error.quit-on-listener-failure")
  val QUIT_ON_EVENT_DECODE_FAILURE = conf.getBoolean("mypipe.error.quit-on-event-decode-failure")
  val QUIT_ON_EVENT_HANDLER_FAILURE = conf.getBoolean("mypipe.error.quit-on-event-handler-failure")
  val QUIT_ON_EMPTY_MUTATION_COMMIT_FAILURE = conf.getBoolean("mypipe.error.quit-on-empty-mutation-commit-failure")

  val INCLUDE_EVENT_CONDITION = conf.getOptionalNoneEmptyString("mypipe.include-event-condition")

  val MYSQL_SERVER_ID_PREFIX = conf.getInt("mypipe.mysql-server-id-prefix")

  private val lastBinlogFilePos = scala.collection.concurrent.TrieMap[String, BinaryLogFilePosition]()

  try {
    new File(DATADIR).mkdirs()
    new File(LOGDIR).mkdirs()
  } catch {
    case e: Exception ⇒ println(s"Error while creating data and log dir $DATADIR, $LOGDIR: ${e.getMessage}")
  }

  def binlogGetStatusFilename(consumerId: String, pipe: String): String = {
    s"$DATADIR/$pipe-$consumerId.pos"
  }

  def binlogLoadFilePosition(consumerId: String, pipeName: String): Option[BinaryLogFilePosition] = {
    try {

      val statusFile = binlogGetStatusFilename(consumerId, pipeName)
      val filePos = scala.io.Source.fromFile(statusFile).getLines().mkString.split(":")
      Some(BinaryLogFilePosition(filePos(0), filePos(1).toLong))

    } catch {
      case e: Exception ⇒ None
    }
  }

  def binlogSaveFilePosition(consumerId: String, filePos: BinaryLogFilePosition, pipe: String): Boolean = {

    try {
      val fileName = binlogGetStatusFilename(consumerId, pipe)

      if (!lastBinlogFilePos.getOrElse(fileName, "").equals(filePos)) {

        val file = new File(fileName)
        val writer = new PrintWriter(file)

        log.info(s"Saving binlog position for pipe $pipe/$consumerId -> $filePos")
        writer.write(s"${filePos.filename}:${filePos.pos}")
        writer.close()

        lastBinlogFilePos(fileName) = filePos
      }

      true
    } catch {
      case e: Exception ⇒
        log.error(s"Failed saving binary log position $filePos for consumer $consumerId and pipe $pipe: ${e.getMessage}\n${e.getStackTraceString}")
        false
    }
  }

  def loadClassesForKey[T](key: String): Map[String, Option[Class[T]]] = {
    val classes = Conf.conf.getObject(key).asScala
    classes.map(kv ⇒ {
      val subKey = kv._1
      val classConf = conf.getConfig(s"$key.$subKey")
      val className = try { Some(classConf.getString("class")) } catch { case e: Exception ⇒ None }
      (subKey, className.map(Class.forName(_).asInstanceOf[Class[T]]))
    }).toMap
  }
}
