package mypipe.avro.schema

import org.apache.avro.Schema

trait AvroSchema {
  def schemaToString(schema: Schema): String = schema.toString(true)
  def stringToSchema(schema: String): Schema = new Schema.Parser().parse(schema)
}

trait StringSchema {
  def schemaToString(schema: String): String = schema
  def stringToSchema(schema: String): String = schema
}