package mypipe.kafka.producer

import java.nio.ByteBuffer

import mypipe.api.data.{Column, ColumnType, Row}
import mypipe.api.event.{Mutation, SingleValuedMutation, UpdateMutation}
import mypipe.avro.schema.AvroSchemaUtils
import org.apache.avro.Schema
import org.apache.avro.generic.GenericData
import mypipe.kafka.KafkaUtil

case class KafkaSpecificAvroSerializer() extends KafkaAvroSerializer {

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

  override protected def validateInsertOrDelete(mutation: SingleValuedMutation, row: Row, schema: Schema): Boolean = {
    val missingFields = row.columns.filterNot(col ⇒ Option(schema.getField(col._1)).isDefined)
    missingFields.isEmpty
  }

  override protected def validateUpdate(mutation: UpdateMutation, row: (Row, Row), schema: Schema): Boolean = {
    val missingFieldsOld = row._1.columns.filterNot(col ⇒ Option(schema.getField("old_" + col._1)).isDefined)
    val missingFieldsNew = row._2.columns.filterNot(col ⇒ Option(schema.getField("new_" + col._1)).isDefined)
    missingFieldsOld.isEmpty && missingFieldsNew.isEmpty
  }

  private def value2Avro(column: Column): Object = {
    column.metadata.colType match {
      case ColumnType.VAR_STRING | ColumnType.STRING ⇒ ByteBuffer.wrap(column.valueOption[Array[Byte]].getOrElse(Array.empty))
      case _                                         ⇒ column.value[Object]
    }
  }
}
