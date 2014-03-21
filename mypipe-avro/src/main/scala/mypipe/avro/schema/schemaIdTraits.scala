package mypipe.avro.schema

trait ShortSchemaId {
  def idToString(id: Short): String = id.toString
  def stringToId(id: String): Short = id.toShort
}

trait IntegerSchemaId {
  def idToString(id: Int): String = id.toString
  def stringToId(id: String): Int = id.toInt
}

trait LongSchemaId {
  def idToString(id: Long): String = id.toString
  def stringToId(id: String): Long = id.toLong
}

trait StringSchemaId {
  def idToString(id: String): String = id
  def stringToId(id: String): String = id
}