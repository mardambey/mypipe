package mypipe.producer

import java.lang.{ Long ⇒ JLong }
import java.util.{ HashMap ⇒ JMap }
import java.nio.ByteBuffer

import com.typesafe.config.Config

import mypipe.api.Conf
import mypipe.api.data.{ Column, ColumnType, Row }
import mypipe.api.event._
import mypipe.api.producer.Producer

import play.api.libs.json._
import play.api.libs.json.Reads._
import play.api.libs.json.Json.JsValueWrapper

import org.slf4j.LoggerFactory

abstract class ProviderProducer {
  def flush(): Boolean
  def send(topic: String, jsonString: String)
}

object EventProducer {
  def apply(config: Config) = new EventProducer(config)
}

class EventProducer(config: Config)
    extends Producer(config = config) {

  protected val logger = LoggerFactory.getLogger(getClass)
  protected val queueProvider = config.getString("queue-provider")
  protected val producer = getProducerFor(queueProvider, config)

  protected def getProducerFor(provider: String, config: Config): ProviderProducer = {
    if (provider == "sqs") {
      val sqsQueue = config.getString("sqs-queue")
      return new SQSProducer(sqsQueue)
    } else {
      val redisConnect = config.getString("redis-connect")
      return new RedisProducer(redisConnect)
    }
  }

  override def handleAlter(event: AlterEvent): Boolean = true // no special support for alters needed, "generic" schema

  override def flush(): Boolean = {
    try {
      producer.flush
      true
    } catch {
      case e: Exception ⇒ {
        logger.error(s"Could not flush producer queue: ${e.getMessage} -> ${e.getStackTraceString}")
        false
      }
    }
  }

  override def queueList(inputList: List[Mutation]): Boolean = {
    inputList.foreach(input ⇒ {
      queue(input)
    })

    true
  }

  override def queue(input: Mutation): Boolean = {
    try {
      jsonRecords(input).foreach(record ⇒ {
        producer.send(getTopic(input), record.toString())
      })

      true
    } catch {
      case e: Exception ⇒ logger.error(s"failed to queue: ${e.getMessage}\n${e.getStackTraceString}"); false
    }
  }

  protected def mutationTypeString(mutation: Mutation): String = Mutation.typeAsString(mutation)

  protected def getTopic(mutation: Mutation): String = TopicUtil.topic(mutation)

  implicit val objectMapFormat = new Format[Map[String, Any]] {

    def writes(map: Map[String, Any]): JsValue =
      Json.obj(map.map {
        case (s, o) ⇒
          val ret: (String, JsValueWrapper) = o match {
            case _: String ⇒ s -> JsString(o.asInstanceOf[String])
            case _: Long   ⇒ s -> JsNumber(o.asInstanceOf[Long])
            case _: Int    ⇒ s -> JsNumber(o.asInstanceOf[Int])
            case _         ⇒ s -> JsArray(o.asInstanceOf[List[String]].map(JsString(_)))
          }
          ret
      }.toSeq: _*)

    def reads(jv: JsValue): JsResult[Map[String, Any]] =
      JsSuccess(jv.as[Map[String, JsValue]].map {
        case (k, v) ⇒
          k -> (v match {
            case s: JsString ⇒ s.as[String]
            case n: JsNumber ⇒ n.as[Long]
            case l           ⇒ l.as[List[String]]
          })
      })
  }

  protected def jsonRecords(mutation: Mutation): List[JsValue] = recordData(mutation).map(record ⇒ Json.toJson(record))

  protected def recordData(mutation: Mutation): List[Map[String, Any]] = {

    Mutation.getMagicByte(mutation) match {

      case Mutation.InsertByte ⇒ mutation.asInstanceOf[InsertMutation].rows.map(row ⇒ {
        if (Conf.INCLUDE_ROW_DATA) {
          val (integers, strings, longs) = columnsToMaps(row.columns)

          header(mutation, row) ++ body(mutation, integers, strings, longs)
        } else {
          header(mutation, row)
        }
      })

      case Mutation.DeleteByte ⇒ mutation.asInstanceOf[DeleteMutation].rows.map(row ⇒ {
        if (Conf.INCLUDE_ROW_DATA) {
          val (integers, strings, longs) = columnsToMaps(row.columns)

          header(mutation, row) ++ body(mutation, integers, strings, longs)
        } else {
          header(mutation, row)
        }
      })

      case Mutation.UpdateByte ⇒ mutation.asInstanceOf[UpdateMutation].rows.map(row ⇒ {
        if (Conf.INCLUDE_ROW_DATA) {
          val (integersOld, stringsOld, longsOld) = columnsToMaps(row._1.columns)
          val (integersNew, stringsNew, longsNew) = columnsToMaps(row._2.columns)

          header(mutation, row._1) ++ body(mutation, integersOld, stringsOld, longsOld) { s ⇒ "old_" + s } ++ body(mutation, integersNew, stringsNew, longsNew) { s ⇒ "new_" + s }
        } else {
          header(mutation, row._1)
        }
      })

      case _ ⇒
        logger.error(s"Unexpected mutation type ${mutation.getClass} encountered; retuning empty List")
        List()
    }
  }

  protected def header(mutation: Mutation, row: Row): Map[String, Any] = {
    val uuidBytes = ByteBuffer.wrap(new Array[Byte](16))
    uuidBytes.putLong(mutation.txid.getMostSignificantBits)
    uuidBytes.putLong(mutation.txid.getLeastSignificantBits)

    val uuidString = uuidBytes.array
      .map { b ⇒ String.format("%02x", new java.lang.Integer(b & 0xff)) }
      .mkString
      .replaceFirst("(\\p{XDigit}{8})(\\p{XDigit}{4})(\\p{XDigit}{4})(\\p{XDigit}{4})(\\p{XDigit}+)", "$1-$2-$3-$4-$5")

    Map[String, Any](
      "database" -> mutation.table.db,
      "table" -> mutation.table.name,
      "tableId" -> mutation.table.id,
      "rowId" -> getRowId(row.columns),
      "mutation" -> mutationTypeString(mutation),
      "txId" -> uuidString,
      "txQueryCount" -> mutation.txQueryCount)
  }

  protected def getRowId(columns: Map[String, Column]): java.lang.Long = {
    var pkId: java.lang.Long = null

    val cols = columns.values.groupBy(_.metadata.colType)

    cols.foreach({

      case (ColumnType.INT24, colz) ⇒
        colz.foreach(c ⇒ {
          val v = c.valueOption[Int]
          if (v.isDefined && c.metadata.isPrimaryKey) pkId = new java.lang.Long(v.get.toLong)
        })

      case (ColumnType.LONG, colz) ⇒
        colz.foreach(c ⇒ {
          // this damn thing can come in as an Integer or Long
          val v = c.value match {
            case i: java.lang.Integer ⇒ new java.lang.Long(i.toLong)
            case l: java.lang.Long    ⇒ l
            case null                 ⇒ null
          }

          if (v != null && c.metadata.isPrimaryKey) pkId = v
        })

      case _ ⇒ // unsupported
    })

    pkId
  }

  protected def body(mutation: Mutation,
                     integers: JMap[CharSequence, Integer],
                     strings: JMap[CharSequence, CharSequence],
                     longs: JMap[CharSequence, JLong])(implicit keyOp: String ⇒ String = s ⇒ s): Map[String, Any] = {
    Map[String, Any](
      keyOp("integers") -> integers,
      keyOp("strings") -> strings,
      keyOp("longs") -> longs)
  }

  protected def columnsToMaps(columns: Map[String, Column]): (JMap[CharSequence, Integer], JMap[CharSequence, CharSequence], JMap[CharSequence, JLong]) = {

    val cols = columns.values.groupBy(_.metadata.colType)

    // ugliness follows... we'll clean it up some day.
    val integers = new java.util.HashMap[CharSequence, Integer]()
    val strings = new java.util.HashMap[CharSequence, CharSequence]()
    val longs = new java.util.HashMap[CharSequence, java.lang.Long]()

    cols.foreach({

      case (ColumnType.INT24, colz) ⇒
        colz.foreach(c ⇒ {
          val v = c.valueOption[Int]
          if (v.isDefined) integers.put(c.metadata.name, v.get)
        })

      case (ColumnType.VARCHAR, colz) ⇒
        colz.foreach(c ⇒ {
          val v = c.valueOption[String]
          if (v.isDefined) strings.put(c.metadata.name, v.get)
        })

      case (ColumnType.LONG, colz) ⇒
        colz.foreach(c ⇒ {
          // this damn thing can come in as an Integer or Long
          val v = c.value match {
            case i: java.lang.Integer ⇒ new java.lang.Long(i.toLong)
            case l: java.lang.Long    ⇒ l
            case null                 ⇒ null
          }

          if (v != null) longs.put(c.metadata.name, v)
        })

      case _ ⇒ // unsupported
    })

    (integers, strings, longs)
  }

  override def toString(): String = {
    s"redis-avro-producer-${producer.toString}"
  }
}
