package mypipe.mysql

case class MySQLBinaryLogConsumer(
  override val hostname: String, override val port: Int,
  username: String, password: String, initialBinlogFileAndPos: BinaryLogFilePosition)

    extends AbstractMySQLBinaryLogConsumer(hostname, port, username, password, initialBinlogFileAndPos)

    with ConfigBasedErrorHandlingBehaviour
    with CacheableTableMapBehaviour

