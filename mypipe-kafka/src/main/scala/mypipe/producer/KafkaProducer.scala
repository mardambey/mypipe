package mypipe.producer

import mypipe.api._
import mypipe.kafka.KafkaAvroVersionedProducer
import com.typesafe.config.Config
import mypipe.avro.schema.{ GenericSchemaRepository, SchemaRepo }
import org.apache.avro.specific.SpecificRecord
import mypipe.avro.{ AvroVersionedRecordSerializer, GenericInMemorySchemaRepo }
import org.apache.avro.Schema
import java.lang.{ Long ⇒ JLong }
import java.util.{ HashMap ⇒ JMap }

/** The base class for a Mypipe producer that encodes Mutation instances
 *  as Avro records and publishes them into Kafka.
 *
 *  @param mappings
 *  @param config
 */
abstract class KafkaMutationAvroProducer(mappings: List[Mapping], config: Config)
    extends Producer(mappings = null, config = config) {

  type InputRecord = SpecificRecord

  protected val schemaRepoClient: GenericSchemaRepository[Short, Schema]
  protected val serializer: Serializer[InputRecord, Array[Byte]]

  protected val metadataBrokers = config.getString("metadata-brokers")
  protected val producer = new KafkaAvroVersionedProducer[InputRecord](metadataBrokers, schemaRepoClient)

  protected val Insert = classOf[mypipe.api.InsertMutation]
  protected val Update = classOf[mypipe.api.UpdateMutation]
  protected val Delete = classOf[mypipe.api.DeleteMutation]

  protected def getTopic(mutation: Mutation[_]): String
  protected def toAvroBean(mutation: Mutation[_]): InputRecord

  override def flush(): Boolean = {
    producer.flush
    true
  }

  override def queueList(inputList: List[Mutation[_]]): Boolean = {
    inputList.foreach(i ⇒ producer.send(getTopic(i), toAvroBean(i)))
    true
  }

  override def queue(input: Mutation[_]): Boolean = {
    producer.send(getTopic(input), toAvroBean(input))
    true
  }

  override def toString(): String = {
    ""
  }

}

/** An implementation of the base KafkaMutationAvroProducer class that uses a
 *  GenericInMemorySchemaRepo in order to encode mutations as Avro beans.
 *  Three beans are encoded: mypipe.avro.InsertMutation, UpdateMutation, and
 *  DeleteMutation. The Kafka topic names are calculated as:
 *  dbName_tableName_(insert|update|delete)
 *
 *  @param mappings
 *  @param config
 */
class KafkaMutationGenericAvroProducer(mappings: List[Mapping], config: Config)
    extends KafkaMutationAvroProducer(mappings, config) {

  override protected val schemaRepoClient = GenericInMemorySchemaRepo
  override protected val serializer = new AvroVersionedRecordSerializer[InputRecord](schemaRepoClient)

  override protected def getTopic(mutation: Mutation[_]): String = mutation.getClass match {
    case Insert ⇒ s"${mutation.table.db}_${mutation.table.name}_insert"
    case Delete ⇒ s"${mutation.table.db}_${mutation.table.name}_delete"
    case Update ⇒ s"${mutation.table.db}_${mutation.table.name}_update"
  }

  override protected def toAvroBean(mutation: Mutation[_]): InputRecord = {

    mutation.getClass match {

      case Insert ⇒ {
        val (integers, strings, longs) = columnsToMaps(mutation.asInstanceOf[InsertMutation].rows.head.columns)
        new mypipe.avro.InsertMutation(
          mutation.table.db,
          mutation.table.name,
          mutation.table.id,
          integers, strings, longs)
      }

      case Delete ⇒ {
        val (integers, strings, longs) = columnsToMaps(mutation.asInstanceOf[DeleteMutation].rows.head.columns)
        new mypipe.avro.DeleteMutation(
          mutation.table.db,
          mutation.table.name,
          mutation.table.id,
          integers, strings, longs)
      }

      case Update ⇒ {
        val (integersOld, stringsOld, longsOld) = columnsToMaps(mutation.asInstanceOf[UpdateMutation].rows.head._1.columns)
        val (integersNew, stringsNew, longsNew) = columnsToMaps(mutation.asInstanceOf[UpdateMutation].rows.head._2.columns)
        new mypipe.avro.UpdateMutation(
          mutation.table.db,
          mutation.table.name,
          mutation.table.id,
          integersOld, stringsOld, longsOld,
          integersNew, stringsNew, longsNew)
      }
    }
  }

  protected def columnsToMaps(columns: Map[String, Column]): (JMap[CharSequence, Integer], JMap[CharSequence, CharSequence], JMap[CharSequence, JLong]) = {

    val cols = columns.values.groupBy(_.metadata.colType)

    val integers = new java.util.HashMap[CharSequence, Integer]()
    val strings = new java.util.HashMap[CharSequence, CharSequence]()
    val longs = new java.util.HashMap[CharSequence, java.lang.Long]()

    cols.foreach(_ match {

      case (ColumnType.INT24, columns) ⇒
        columns.foreach(c ⇒ integers.put(c.metadata.name, c.value[Int]))

      case (ColumnType.VARCHAR, columns) ⇒
        columns.foreach(c ⇒ strings.put(c.metadata.name, c.value[String]))

      case (ColumnType.LONG, columns) ⇒
        columns.foreach(c ⇒ {
          // this damn thing can come in as an Integer or Long
          val v = c.value match {
            case i: java.lang.Integer ⇒ new java.lang.Long(i.toLong)
            case l: java.lang.Long    ⇒ l
          }

          longs.put(c.metadata.name, v)
        })

      case _ ⇒ // unsupported
    })

    (integers, strings, longs)
  }
}

class KafkaMutationSpecificAvroProducer(mappings: List[Mapping], config: Config)
    extends KafkaMutationAvroProducer(mappings, config) {

  private val schemaRepoClientClassName = config.getString("schema-repo-client")

  override protected val serializer = new AvroVersionedRecordSerializer[InputRecord](schemaRepoClient)
  override protected val schemaRepoClient = Class.forName(schemaRepoClientClassName)
    .newInstance()
    .asInstanceOf[GenericSchemaRepository[Short, Schema]]

  override protected def getTopic(mutation: Mutation[_]): String = ???
  override protected def toAvroBean(mutation: Mutation[_]): InputRecord = mutation.getClass match {
    // TODO: implement this properly
    case Insert ⇒ new mypipe.avro.InsertMutation()
    case Update ⇒ new mypipe.avro.UpdateMutation()
    case Delete ⇒ new mypipe.avro.DeleteMutation()
  }
}
