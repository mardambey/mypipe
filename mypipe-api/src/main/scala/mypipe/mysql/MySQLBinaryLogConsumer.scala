package mypipe.mysql

case class MySQLBinaryLogConsumer(
  override protected val hostname: String, override protected val port: Int,
  override protected val username: String, override protected val password: String)

    extends AbstractMySQLBinaryLogConsumer(hostname, port, username, password)

    with ConfigBasedErrorHandlingBehaviour
    with CacheableTableMapBehaviour

