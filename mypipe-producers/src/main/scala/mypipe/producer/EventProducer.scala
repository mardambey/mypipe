package mypipe.producer

import java.io.ByteArrayOutputStream
import java.lang.{ Long ⇒ JLong }
import java.util.{ HashMap ⇒ JMap }
import java.nio.ByteBuffer

import com.typesafe.config.Config

import mypipe.api.Conf
import mypipe.api.data.{ Column, ColumnType, Row }
import mypipe.api.event._
import mypipe.avro.schema.AvroSchemaUtils
import mypipe.avro.GenericInMemorySchemaRepo
import mypipe.api.producer.Producer

import org.apache.avro.Schema
import org.apache.avro.generic.{ GenericRecord, GenericDatumWriter, GenericData }
import org.apache.avro.io.EncoderFactory

import org.slf4j.LoggerFactory

abstract class ProviderProducer {
  def flush(): Boolean
  def send(topic: String, jsonString: String)
}

object EventProducer {
  def apply(config: Config) = new EventProducer(config)
}

/** An implementation of the base RedisMutationAvroProducer class that uses a
 *  GenericInMemorySchemaRepo in order to encode mutations as Avro beans.
 *  Three beans are encoded: mypipe.avro.InsertMutation, UpdateMutation, and
 *  DeleteMutation. The Redis event names are calculated as:
 *  dbName_tableName_(insert|update|delete)
 *
 *  @param config configuration must have "redis-connect"
 */
class EventProducer(config: Config)
    extends Producer(config = config) {

  type OutputType = Array[Byte]

  protected val schemaRepoClient = GenericInMemorySchemaRepo

  protected val queueProvider = config.getString("queue-provider")
  protected val producer = getProducerFor(queueProvider, config)

  protected val logger = LoggerFactory.getLogger(getClass)
  protected val encoderFactory = EncoderFactory.get()

  override def handleAlter(event: AlterEvent): Boolean = true // no special support for alters needed, "generic" schema

  /** Given a short, returns a byte array.
   *
   *  @param s schema id
   *  @return
   */
  protected def schemaIdToByteArray(s: Short) = Array[Byte](((s & 0xFF00) >> 8).toByte, (s & 0x00FF).toByte)

  /** Given a mutation, returns a string (for example: insert, update, delete).
   *
   *  @param mutation
   *  @return
   */
  protected def mutationTypeString(mutation: Mutation): String = Mutation.typeAsString(mutation)

  /** Builds the Redis topic using the mutation's database, table name, and
   *  the mutation type (insert, update, delete) concatenating them with "_"
   *  as a delimeter.
   *
   *  @param mutation
   *  @return the topic name
   */
  protected def getTopic(mutation: Mutation): String = TopicUtil.topic(mutation)

  /** Given a Mutation, this method must convert it into a(n) Avro record(s)
   *  for the given Avro schema.
   *
   *  @param mutation
   *  @param schema
   *  @return the Avro generic record(s)
   */
  protected def avroRecord(mutation: Mutation, schema: Schema): List[GenericData.Record] = {

    Mutation.getMagicByte(mutation) match {

      case Mutation.InsertByte ⇒ mutation.asInstanceOf[InsertMutation].rows.map(row ⇒ {
        val record = new GenericData.Record(schema)
        header(record, mutation, row)

        if (Conf.INCLUDE_ROW_DATA) {
          val (integers, strings, longs) = columnsToMaps(row.columns)
          body(record, mutation, integers, strings, longs)
        }

        record
      })

      case Mutation.DeleteByte ⇒ mutation.asInstanceOf[DeleteMutation].rows.map(row ⇒ {
        val record = new GenericData.Record(schema)
        header(record, mutation, row)

        if (Conf.INCLUDE_ROW_DATA) {
          val (integers, strings, longs) = columnsToMaps(row.columns)
          body(record, mutation, integers, strings, longs)
        }

        record
      })

      case Mutation.UpdateByte ⇒ mutation.asInstanceOf[UpdateMutation].rows.map(row ⇒ {
        val record = new GenericData.Record(schema)
        header(record, mutation, row._1)

        if (Conf.INCLUDE_ROW_DATA) {
          val (integersOld, stringsOld, longsOld) = columnsToMaps(row._1.columns)
          val (integersNew, stringsNew, longsNew) = columnsToMaps(row._2.columns)
          body(record, mutation, integersOld, stringsOld, longsOld) { s ⇒ "old_" + s }
          body(record, mutation, integersNew, stringsNew, longsNew) { s ⇒ "new_" + s }
        }

        record
      })

      case _ ⇒
        logger.error(s"Unexpected mutation type ${mutation.getClass} encountered; retuning empty Avro GenericData.Record(schema=$schema")
        List(new GenericData.Record(schema))
    }
  }

  protected def body(record: GenericData.Record,
                     mutation: Mutation,
                     integers: JMap[CharSequence, Integer],
                     strings: JMap[CharSequence, CharSequence],
                     longs: JMap[CharSequence, JLong])(implicit keyOp: String ⇒ String = s ⇒ s) {
    record.put(keyOp("integers"), integers)
    record.put(keyOp("strings"), strings)
    record.put(keyOp("longs"), longs)
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

  /** Given an Avro generic record, schema, and schemaId, serialized
   *  them into an array of bytes.
   *
   *  @param record
   *  @param schema
   *  @param schemaId
   *  @return
   */
  protected def serialize(record: GenericData.Record, schema: Schema, schemaId: Short): String = {
    val encoderFactory = EncoderFactory.get()
    val writer = new GenericDatumWriter[GenericRecord]()
    writer.setSchema(schema)
    val out = new ByteArrayOutputStream()

    val enc = encoderFactory.jsonEncoder(schema, out)
    writer.write(record, enc)
    enc.flush

    out.toString
  }

  /** Given a mutation, returns the "subject" that this mutation's
   *  Schema is registered under in the Avro schema repository.
   *
   *  @param mutation mutation to get subject for
   *  @return
   */
  protected def avroSchemaSubject(mutation: Mutation): String = AvroSchemaUtils.genericSubject(mutation)

  override def toString(): String = {
    s"redis-avro-producer-${producer.toString}"
  }

  protected def getProducerFor(provider: String, config: Config): ProviderProducer = {
    if (provider == "sqs") {
      val sqsQueue = config.getString("sqs-queue")
      return new SQSProducer(sqsQueue)
    } else {
      val redisConnect = config.getString("redis-connect")
      return new RedisProducer(redisConnect)
    }
  }

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

  /** Adds a header into the given Record based on the Mutation's
   *  database, table, and tableId.
   *
   *  @param record
   *  @param mutation
   */
  protected def header(record: GenericData.Record, mutation: Mutation, row: Row) {
    record.put("database", mutation.table.db)
    record.put("table", mutation.table.name)
    record.put("tableId", mutation.table.id)
    record.put("rowId", getRowId(row.columns))
    record.put("mutation", mutationTypeString(mutation))

    // TODO: avoid null check
    if (mutation.txid != null && record.getSchema().getField("txid") != null) {
      val uuidBytes = ByteBuffer.wrap(new Array[Byte](16))
      uuidBytes.putLong(mutation.txid.getMostSignificantBits)
      uuidBytes.putLong(mutation.txid.getLeastSignificantBits)

      val uuidString = uuidBytes.array.map { b ⇒ String.format("%02x", new java.lang.Integer(b & 0xff)) }.mkString.replaceFirst("(\\p{XDigit}{8})(\\p{XDigit}{4})(\\p{XDigit}{4})(\\p{XDigit}{4})(\\p{XDigit}+)", "$1-$2-$3-$4-$5")

      record.put("txid", uuidString)
      record.put("txQueryCount", mutation.txQueryCount)
    }
  }

  override def queueList(inputList: List[Mutation]): Boolean = {
    inputList.foreach(input ⇒ {
      val schemaTopic = avroSchemaSubject(input)
      val schema = schemaRepoClient.getLatestSchema(schemaTopic).get
      val schemaId = schemaRepoClient.getSchemaId(schemaTopic, schema)
      val records = avroRecord(input, schema)

      records foreach (record ⇒ {
        val jsonString = serialize(record, schema, schemaId.get)
        producer.send(getTopic(input), jsonString)
      })
    })

    true
  }

  override def queue(input: Mutation): Boolean = {
    try {
      val schemaTopic = avroSchemaSubject(input)
      val schema = schemaRepoClient.getLatestSchema(schemaTopic).get
      val schemaId = schemaRepoClient.getSchemaId(schemaTopic, schema)
      val records = avroRecord(input, schema)

      records foreach (record ⇒ {
        val jsonString = serialize(record, schema, schemaId.get)
        producer.send(getTopic(input), jsonString)
      })

      true
    } catch {
      case e: Exception ⇒ logger.error(s"failed to queue: ${e.getMessage}\n${e.getStackTraceString}"); false
    }
  }
}
