package mypipe.producer

import java.nio.ByteBuffer

import com.typesafe.config.Config
import mypipe.api.data.{ Row, ColumnType, Column }
import mypipe.api.event.{ AlterEvent, UpdateMutation, SingleValuedMutation, Mutation }
import mypipe.avro.schema.{ AvroSchemaUtils, GenericSchemaRepository }
import mypipe.avro.AvroVersionedRecordSerializer
import mypipe.kafka.KafkaUtil
import org.apache.avro.Schema
import org.apache.avro.generic.GenericData

class KafkaMutationSpecificAvroProducer(config: Config)
    extends KafkaMutationAvroProducer[Short](config) {

  private val schemaRepoClientClassName = config.getString("schema-repo-client")

  override protected val schemaRepoClient = Class.forName(schemaRepoClientClassName + "$")
    .getField("MODULE$").get(null)
    .asInstanceOf[GenericSchemaRepository[Short, Schema]]

  override protected val serializer = new AvroVersionedRecordSerializer[InputRecord](schemaRepoClient)

  override def handleAlter(event: AlterEvent): Boolean = {
    // FIXME: if the table is not in the cache already, by it's ID, this will fail
    // refresh insert, update, and delete schemas
    (for (
      i ← schemaRepoClient.getLatestSchema(AvroSchemaUtils.specificSubject(event.database, event.table.name, Mutation.InsertString), flushCache = true);
      u ← schemaRepoClient.getLatestSchema(AvroSchemaUtils.specificSubject(event.database, event.table.name, Mutation.UpdateString), flushCache = true);
      d ← schemaRepoClient.getLatestSchema(AvroSchemaUtils.specificSubject(event.database, event.table.name, Mutation.DeleteString), flushCache = true)
    ) yield {
      true
    }).getOrElse(false)
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
    KafkaUtil.specificTopic(mutation)

  /** Given a mutation, returns the Avro subject that this mutation's
   *  Schema is registered under in the Avro schema repository.
   *
   *  @param mutation mutation to get subject for
   *  @return returns "mutationDbName_mutationTableName_mutationType" where mutationType is "insert", "update", or "delete"
   */
  override protected def avroSchemaSubject(mutation: Mutation): String = AvroSchemaUtils.specificSubject(mutation)

  override protected def body(record: GenericData.Record, row: Row, schema: Schema)(implicit keyOp: String ⇒ String = s ⇒ s) {
    row.columns.foreach(col ⇒ Option(schema.getField(keyOp(col._1))).foreach(f ⇒ record.put(f.name(), value2Avro(col._2))))
  }

  private def value2Avro(column: Column): Object = {
    column.metadata.colType match {
      case ColumnType.VAR_STRING | ColumnType.STRING ⇒ ByteBuffer.wrap(column.valueOption[Array[Byte]].getOrElse(Array.empty))
      case _                                         ⇒ column.value[Object]
    }
  }
}
