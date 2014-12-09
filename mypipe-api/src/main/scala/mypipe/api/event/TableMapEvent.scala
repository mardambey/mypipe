package mypipe.api.event

case class TableMapEvent(tableId: Long, tableName: String, database: String, columnTypes: Array[Byte])
