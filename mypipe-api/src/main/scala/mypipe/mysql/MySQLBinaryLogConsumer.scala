package mypipe.mysql

import com.github.shyiko.mysql.binlog.event.{ Event ⇒ MEvent, _ }

case class MySQLBinaryLogConsumer(
  override protected val hostname: String, override protected val port: Int,
  override protected val username: String, override protected val password: String)

    extends AbstractMySQLBinaryLogConsumer(hostname, port, username, password)

    with ConfigBasedErrorHandlingBehaviour[MEvent, BinaryLogFilePosition]
    with ConfigBasedEventSkippingBehaviour
    with CacheableTableMapBehaviour

