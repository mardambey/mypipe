package mypipe.avro

import mypipe.avro.schema.GenericSchemaRepository
import org.schemarepo.{ ValidatorFactory, InMemoryRepository }

abstract class InMemorySchemaRepo[ID, SCHEMA] extends GenericSchemaRepository[ID, SCHEMA] {

  // Configuration
  override protected def getRepositoryURL: String = "InMemoryRepository"

  override lazy val client = new InMemoryRepository(new ValidatorFactory.Builder().build())
}