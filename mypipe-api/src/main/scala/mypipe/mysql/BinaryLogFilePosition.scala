package mypipe.mysql

case class BinaryLogFilePosition(filename: String, pos: Long) {
  override def toString(): String = s"$filename:$pos"
  override def equals(o: Any): Boolean = o.asInstanceOf[BinaryLogFilePosition].filename.equals(filename) && o.asInstanceOf[BinaryLogFilePosition].pos == pos
}

object BinaryLogFilePosition {
  val current = BinaryLogFilePosition("", 0)
}