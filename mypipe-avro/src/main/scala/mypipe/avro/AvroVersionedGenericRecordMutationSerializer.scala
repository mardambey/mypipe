package mypipe.avro

import org.apache.avro.Schema
import org.apache.avro.generic.GenericData
import mypipe.api._
import mypipe.avro.schema.SchemaRepo
import scala.collection.mutable
import java.util.logging.Logger

trait AvroVersionedGenericRecordMutationSerializer extends AvroRecordMutationSerializer {

  val log = Logger.getLogger(getClass.getName)
  val repo = SchemaRepo
  val topicToTableId = mutable.HashMap[String, Long]()

  override protected def getSchemaAndIdForMutation(mutation: Mutation[_]): Option[(Short, Schema)] = {

    // FIXME: this should be pulled out into a common call
    val topicName = s"${mutation.table.db}_${mutation.table.name}_${mutation2String(mutation)}"
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
        try { record.put(nameMapper(column._1), column._2.value) }
        catch { case e: Exception ⇒ log.severe(s"Could not set field ${nameMapper(column._1)} with value ${column._2.value}") }
      })
  }

  protected def mutation2String(mutation: Mutation[_]): String = {
    mutation match {
      case i: InsertMutation ⇒ "insert"
      case u: UpdateMutation ⇒ "update"
      case d: DeleteMutation ⇒ "delete"
    }
  }
}
