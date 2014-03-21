package mypipe.avro.schema

import org.apache.avro.Schema

/** Mypipe-specific implementation of the Schema repo client.
 */
object SchemaRepo
    extends GenericSchemaRepository[Short, Schema]
    with ShortSchemaId
    with AvroSchema {

  def getRepositoryURL: String = {
    ???
  }
}