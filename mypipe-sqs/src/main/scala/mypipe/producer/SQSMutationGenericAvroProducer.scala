package mypipe.producer

import com.typesafe.config.Config
import mypipe.api.data.{ Column, ColumnType }
import mypipe.api.event._
import mypipe.avro.schema.AvroSchemaUtils
import mypipe.avro.{ AvroVersionedRecordSerializer, GenericInMemorySchemaRepo }
import mypipe.sqs.SQSUtil
import org.apache.avro.Schema
import java.lang.{ Long ⇒ JLong }
import java.util.{ HashMap ⇒ JMap }
import org.apache.avro.generic.GenericData

object SQSMutationGenericAvroProducer {
  def apply(config: Config) = new SQSMutationGenericAvroProducer(config)
}

/** An implementation of the base SQSMutationAvroProducer class that uses a
 *  GenericInMemorySchemaRepo in order to encode mutations as Avro beans.
 *  Three beans are encoded: mypipe.avro.InsertMutation, UpdateMutation, and
 *  DeleteMutation.
 *
 *  @param config configuration must have "sqs-queue"
 */
class SQSMutationGenericAvroProducer(config: Config)
    extends SQSMutationAvroProducer[Short](config) {

  override protected val schemaRepoClient = GenericInMemorySchemaRepo
  override protected val serializer = new AvroVersionedRecordSerializer[InputRecord](schemaRepoClient)

  override def handleAlter(event: AlterEvent): Boolean = true // no special support for alters needed, "generic" schema

  /** Given a short, returns a byte array.
   *  @param s schema id
   *  @return
   */
  override protected def schemaIdToByteArray(s: Short) = Array[Byte](((s & 0xFF00) >> 8).toByte, (s & 0x00FF).toByte)

  override protected def getSQSTopic(mutation: Mutation): String = SQSUtil.topic(mutation)

  override protected def avroRecord(mutation: Mutation, schema: Schema): List[GenericData.Record] = {

    Mutation.getMagicByte(mutation) match {

      case Mutation.InsertByte ⇒ mutation.asInstanceOf[InsertMutation].rows.map(row ⇒ {
        val (integers, strings, longs) = columnsToMaps(row.columns)
        val record = new GenericData.Record(schema)
        header(record, mutation)
        body(record, mutation, integers, strings, longs)
        record
      })

      case Mutation.DeleteByte ⇒ mutation.asInstanceOf[DeleteMutation].rows.map(row ⇒ {
        val (integers, strings, longs) = columnsToMaps(row.columns)
        val record = new GenericData.Record(schema)
        header(record, mutation)
        body(record, mutation, integers, strings, longs)
        record
      })

      case Mutation.UpdateByte ⇒ mutation.asInstanceOf[UpdateMutation].rows.map(row ⇒ {
        val (integersOld, stringsOld, longsOld) = columnsToMaps(row._1.columns)
        val (integersNew, stringsNew, longsNew) = columnsToMaps(row._2.columns)
        val record = new GenericData.Record(schema)
        header(record, mutation)
        body(record, mutation, integersOld, stringsOld, longsOld) { s ⇒ "old_" + s }
        body(record, mutation, integersNew, stringsNew, longsNew) { s ⇒ "new_" + s }
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

  /** Given a mutation, returns the "subject" that this mutation's
   *  Schema is registered under in the Avro schema repository.
   *  @param mutation mutation to get subject for
   *  @return
   */
  override protected def avroSchemaSubject(mutation: Mutation): String = AvroSchemaUtils.genericSubject(mutation)
}
