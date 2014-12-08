package mypipe.mysql

case class MySQLBinaryLogConsumer(
  override val hostname: String, override val port: Int,
  username: String, password: String, binlogFileAndPos: BinaryLogFilePosition)

    extends AbstractMySQLBinaryLogConsumer(hostname, port, username, password, binlogFileAndPos)

    with ConfigBasedErrorHandlingBehaviour
    with CacheableTableMapBehaviour

