package mypipe.snapshotter

import scala.concurrent.duration._
import com.github.mauricio.async.db.Connection
import com.typesafe.config.ConfigFactory
import org.slf4j.LoggerFactory

import scala.concurrent.Await

object Snapshotter extends App {

  protected val log = LoggerFactory.getLogger(getClass)
  protected val conf = ConfigFactory.load()

  sys.addShutdownHook({
    log.info("Shutting down...")
  })

}

