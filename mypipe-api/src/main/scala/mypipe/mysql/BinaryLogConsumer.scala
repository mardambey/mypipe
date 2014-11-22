package mypipe.mysql

import mypipe.api.{ Table, TableMapEvent, Mutation }

case class BinaryLogConsumer(
  override val hostname: String,
  override val port: Int,
  username: String,
  password: String,
  binlogFileAndPos: BinaryLogFilePosition)
    extends BaseBinaryLogConsumer(hostname, port, username, password, binlogFileAndPos)
    with ConfigBasedErrorHandling
    with CacheableTableMap {

  override protected def handleMutation(mutation: Mutation[_]): Boolean = {
    val results = listeners.takeWhile(l ⇒
      try {
        l.onMutation(this, mutation)
      } catch {
        case e: Exception ⇒
          log.error("Listener $l failed on mutation from event: $event")
          handleError(l, mutation)
          false
      })

    if (results.size == listeners.size) true
    else false
  }

  override protected def handleTableMap(tableMapEvent: TableMapEvent): Table = {
    // FIXME: this breaks the dependency injection
    val table = super.handleTableMap(tableMapEvent)
    listeners.foreach(_.onTableMap(this, table))
    table
  }

  override protected def handleDisconnect {
    listeners foreach (l ⇒ l.onDisconnect(this))
  }

  override protected def handleConnect {
    listeners foreach (l ⇒ l.onConnect(this))
  }

  def registerListener(listener: BinaryLogConsumerListener) {
    listeners += listener
  }
}

