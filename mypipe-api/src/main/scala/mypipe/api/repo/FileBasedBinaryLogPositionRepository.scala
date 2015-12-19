package mypipe.api.repo

import java.io.{ File, PrintWriter }

import com.typesafe.config.Config
import mypipe.api.Conf
import mypipe.api.consumer.BinaryLogConsumer
import mypipe.mysql.BinaryLogFilePosition
import mypipe.api.Conf.RichConfig
import org.slf4j.LoggerFactory

class FileBasedBinaryLogPositionRepository(filePrefix: String, dataDir: String) extends BinaryLogPositionRepository {

  protected val log = LoggerFactory.getLogger(getClass)

  protected var lastBinlogFilePos: Option[BinaryLogFilePosition] = None

  // ensure that data directory exists
  new File(dataDir).mkdirs()

  override def saveBinaryLogPosition(consumer: BinaryLogConsumer[_]): Unit = {
    val fileName = binlogGetStatusFilename(consumer.id, filePrefix)
    log.info(s"Saving binlog position for pipe $filePrefix/${consumer.id} -> ${consumer.getBinaryLogPosition}")
    binlogSaveFilePositionToFile(consumer, fileName)
  }

  override def loadBinaryLogPosition(consumer: BinaryLogConsumer[_]): Option[BinaryLogFilePosition] = {
    try {

      val statusFile = binlogGetStatusFilename(consumer.id, filePrefix)
      val filePos = scala.io.Source.fromFile(statusFile).getLines().mkString.split(":")
      Some(BinaryLogFilePosition(filePos(0), filePos(1).toLong))

    } catch {
      case e: Exception ⇒ None
    }
  }

  private def binlogGetStatusFilename(consumerId: String, fileNamePrefix: String): String = {
    s"$dataDir/$fileNamePrefix-$consumerId.pos"
  }

  private def binlogSaveFilePositionToFile(consumer: BinaryLogConsumer[_], fileName: String): Boolean = {

    val consumerId = consumer.id
    val filePos = consumer.getBinaryLogPosition.getOrElse({
      log.warn(s"Tried saving non-existent binary log position for consumer $consumerId, saving ${BinaryLogFilePosition.current} instead.")
      BinaryLogFilePosition.current
    })

    try {

      if (!lastBinlogFilePos.exists(_.equals(filePos))) {

        val file = new File(fileName)
        val writer = new PrintWriter(file)

        writer.write(s"${filePos.filename}:${filePos.pos}")
        writer.close()

        lastBinlogFilePos = Some(filePos)
      }

      true
    } catch {
      case e: Exception ⇒
        log.error(s"Failed saving binary log position $filePos for consumer $consumerId to file $fileName: ${e.getMessage}\n${e.getStackTraceString}")
        false
    }
  }
}

class ConfigurableFileBasedBinaryLogPositionRepository(override val config: Config)
  extends FileBasedBinaryLogPositionRepository(
    config.getString("file-prefix"),
    config.getOptionalNoneEmptyString("data-dir").getOrElse(Conf.DATADIR))
  with ConfigurableBinaryLogPositionRepository

