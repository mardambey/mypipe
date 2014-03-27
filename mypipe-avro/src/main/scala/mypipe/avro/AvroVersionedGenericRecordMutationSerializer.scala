package mypipe.avro

import org.apache.avro.Schema
import org.apache.avro.generic.GenericData
import mypipe.api._
import mypipe.avro.schema.SchemaRepo
import scala.collection.mutable

trait AvroVersionedGenericRecordMutationSerializer extends AvroRecordMutationSerializer {

  val repo = SchemaRepo
  val topicToTableId = mutable.HashMap[String, Long]()

  override protected def getSchemaAndIdForMutation(mutation: Mutation[_]): Option[(Short, Schema)] = {

    // FIXME: this should be pulled out into a common call
    val topicName = mutation.table.db + "__" + mutation.table.name
    val tableId = topicToTableId.getOrElse(topicName, -1)
    val flushCache = (mutation.table.id != tableId)

    repo.getLatestSchema(topicName, flushCache) match {
      case Some(schema) ⇒ {
        val id = repo.getSchemaId(topicName, schema)

        if (id.isDefined) Some(id.get -> schema)
        else None
      }

      case None ⇒ None
    }
  }

  override protected def writeColumns(record: GenericData.Record, columns: Map[String, Column], nameMapper: NameMapper = IdentityNameMapper) {
    columns.foreach(
      column ⇒ {
        record.put(nameMapper(column._1), column._2.value)
      })
  }
}
