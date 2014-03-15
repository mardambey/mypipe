package mypipe.mysql

case class BinlogFilePos(filename: String, pos: Long) {
  override def toString(): String = s"$filename:$pos"
  override def equals(o: Any): Boolean = o.asInstanceOf[BinlogFilePos].filename.equals(filename) && o.asInstanceOf[BinlogFilePos].pos == pos
}

object BinlogFilePos {
  val current = BinlogFilePos("", 0)
}