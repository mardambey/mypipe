package mypipe

import java.util.logging.{ FileHandler, ConsoleHandler, Level, Logger }

object Log {
  val log = Logger.getLogger(getClass.getName)
  log.setUseParentHandlers(false)
  log.setLevel(Level.ALL)

  val handlers = List(new ConsoleHandler, new FileHandler(s"${Conf.LOGDIR}/mypipe.log", true /* append */ ))

  handlers.foreach(h ⇒ {
    h.setFormatter(new LogFormatter())
    log.addHandler(h)
  })

  sys.addShutdownHook({
    handlers.foreach(_.close())
  })

  def info(str: String) = log.info(str)
  def warning(str: String) = log.warning(str)
  def severe(str: String) = log.severe(str)
  def fine(str: String) = log.fine(str)
  def finer(str: String) = log.finer(str)
  def finest(str: String) = log.finest(str)

  import java.io.PrintWriter
  import java.io.StringWriter
  import java.util.Date
  import java.util.logging.Formatter
  import java.util.logging.LogRecord

  private class LogFormatter extends Formatter {

    val LINE_SEPARATOR = System.getProperty("line.separator")

    override def format(record: LogRecord): String = {
      val sb = new StringBuilder()

      sb.append(new Date(record.getMillis()))
        .append(" [")
        .append(record.getLevel().getLocalizedName())
        .append("] ")
        .append(formatMessage(record))
        .append(LINE_SEPARATOR)

      if (record.getThrown() != null) {
        try {
          val sw = new StringWriter()
          val pw = new PrintWriter(sw)
          record.getThrown().printStackTrace(pw)
          pw.close()
          sb.append(sw.toString())
        } catch {
          case t: Throwable ⇒ // ignore
        }
      }

      return sb.toString()
    }
  }
}