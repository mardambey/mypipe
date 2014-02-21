package mypipe.mapper

import mypipe.Mutation

case class KS(keySpace: String) {
  def CF[K, V](cf: String) = new CF(this, cf)
}

case class Row(cf: CF, key: String) {

  def put(kv: (String, String)): Row = {
    this
  }

  def incr(key: String): Row = {
    this
  }

  def decr(key: String): Row = {
    this
  }
}

case class CF(ks: KS, cf: String) {
  def row(rowKey: String): Row = {
    Row(this, rowKey)
  }
}

trait Mapping[T] {
  def map(mutation: Mutation): T
}

class ProfileMapping extends Mapping[Object] {

  val ks = KS("my_keyspace")
  val profiles = ks.CF[String, String]("profiles")
  val counters = ks.CF[Long, String]("counters")

  def map(mutation: Mutation): Object = {

    profiles
      .row("1501571")
      .put("nick_name" -> "hisham320")
      .put("email" -> "hmb@mate1.com")

    counters
      .row("1501571")
      .incr("login_count")
      .decr("free_offers")

    null
  }
}

object CassandraDSLTest extends App {

}