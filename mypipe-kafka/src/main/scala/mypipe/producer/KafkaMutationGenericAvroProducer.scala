package mypipe.producer

import java.nio.ByteBuffer

import com.typesafe.config.Config
import mypipe.api.data.{ Row, Column, ColumnType }
import mypipe.api.event._
import mypipe.avro.schema.AvroSchemaUtils
import mypipe.avro.{ AvroVersionedRecordSerializer, GenericInMemorySchemaRepo }
import mypipe.kafka.KafkaUtil
import org.apache.avro.Schema
import java.lang.{ Long ⇒ JLong }
import java.util.{ HashMap ⇒ JMap }
import org.apache.avro.generic.GenericData

object KafkaMutationGenericAvroProducer {
  def apply(config: Config) = new KafkaMutationGenericAvroProducer(config)
}

/** An implementation of the base KafkaMutationAvroProducer class that uses a
 *  GenericInMemorySchemaRepo in order to encode mutations as Avro beans.
 *  Three beans are encoded:
 *  InsertMutation
 *  UpdateMutation
 *  DeleteMutation
 *
 *  The Kafka topic names are calculated as:
 *  dbName_tableName_(insert|update|delete)
 *
 *  @param config configuration must have "metadata-brokers"
 */
class KafkaMutationGenericAvroProducer(config: Config)
    extends KafkaMutationAvroProducer[Short](config) {

  override protected val schemaRepoClient = GenericInMemorySchemaRepo
  override protected val serializer = new AvroVersionedRecordSerializer[InputRecord](schemaRepoClient)

  override def handleAlter(event: AlterEvent): Boolean = {
    // no special support for alters needed, "generic" schema
    true
  }

  /** Given a schema ID of type SchemaId, converts it to a byte array.
   *
   *  @param s schema id
   *  @return
   */
  override protected def schemaIdToByteArray(s: Short) =
    Array[Byte](((s & 0xFF00) >> 8).toByte, (s & 0x00FF).toByte)

  /** Builds the Kafka topic using the mutation's database, table name, and
   *  the mutation type (insert, update, delete) concatenating them with "_"
   *  as a delimeter.
   *
   *  @param mutation mutation
   *  @return the topic name
   */
  override protected def getKafkaTopic(mutation: Mutation): String =
    KafkaUtil.genericTopic(mutation)

  /** Given a mutation, returns the "subject" that this mutation's
   *  Schema is registered under in the Avro schema repository.
   *
   *  @param mutation mutation to get subject for
   *  @return
   */
  override protected def avroSchemaSubject(mutation: Mutation): String =
    AvroSchemaUtils.genericSubject(mutation)

  override protected def body(record: GenericData.Record, row: Row, schema: Schema)(implicit keyOp: String ⇒ String = s ⇒ s) {
    val (byteArrays, integers, strings, longs) = columnsToMaps(row.columns)
    record.put(keyOp("bytes"), byteArrays)
    record.put(keyOp("integers"), integers)
    record.put(keyOp("strings"), strings)
    record.put(keyOp("longs"), longs)
  }

  protected def columnsToMaps(columns: Map[String, Column]): (JMap[CharSequence, ByteBuffer], JMap[CharSequence, Integer], JMap[CharSequence, CharSequence], JMap[CharSequence, JLong]) = {

    val cols = columns.values.groupBy(_.metadata.colType)

    // ugliness follows... we'll clean it up some day.
    val byteArrays = new java.util.HashMap[CharSequence, ByteBuffer]()
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

      case (ColumnType.VAR_STRING, colz) ⇒
        colz.foreach(c ⇒ {
          val v = c.valueOption[Array[Byte]].map(ByteBuffer.wrap)
          if (v.isDefined) byteArrays.put(c.metadata.name, v.get)
        })

      case _ ⇒ // unsupported
    })

    (byteArrays, integers, strings, longs)
  }
}
