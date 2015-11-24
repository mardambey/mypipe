package mypipe.mysql

case class BinaryLogFilePosition(filename: String, pos: Long) {
  override def toString(): String = s"$filename:$pos"
  override def equals(o: Any): Boolean = {
    o != null &&
      filename.equals(o.asInstanceOf[BinaryLogFilePosition].filename) &&
      pos.equals(o.asInstanceOf[BinaryLogFilePosition].pos)
  }
}

object BinaryLogFilePosition {
  val current = BinaryLogFilePosition("", 0)
}