package mypipe.producer

import mypipe.api._
import mypipe.kafka.{ KafkaProducer ⇒ KProducer, KafkaAvroVersionedProducer }
import com.typesafe.config.Config
import mypipe.avro.schema.{ GenericSchemaRepository, SchemaRepo }
import org.apache.avro.specific.SpecificRecord
import mypipe.avro.{ AvroVersionedRecordSerializer, GenericInMemorySchemaRepo }
import org.apache.avro.Schema

//abstract class KafkaProducer[Output](mappings: List[Mapping], config: Config) extends Producer(mappings = null, config = config) {
//
//  type Input = Mutation[_]
//
//  val producer: KProducer[Input, Output]
//
//  protected def getTopic(input: Input): String
//
//  override def flush(): Boolean = {
//    producer.flush
//    true
//  }
//
//  override def queueList(inputList: List[Input]): Boolean = {
//    inputList.foreach(i => producer.send(getTopic(i), i))
//    true
//  }
//
//  override def queue(input: Input): Boolean = {
//    producer.send(getTopic(input), input)
//    true
//  }
//
//  override def toString(): String = {
//    ""
//  }
//
//}

abstract class KafkaMutationAvroProducer(mappings: List[Mapping], config: Config)
    extends Producer(mappings = null, config = config) {

  type InputRecord = SpecificRecord

  protected val schemaRepoClient: GenericSchemaRepository[Short, Schema]
  protected val serializer: Serializer[InputRecord, Array[Byte]]

  protected val metadataBrokers = config.getString("metadata-brokers")
  protected val producer = new KafkaAvroVersionedProducer[InputRecord](metadataBrokers, schemaRepoClient)

  protected val Insert = classOf[InsertMutation]
  protected val Update = classOf[UpdateMutation]
  protected val Delete = classOf[DeleteMutation]

  protected def getTopic(mutation: Mutation[_]): String
  protected def getRecord(mutation: Mutation[_]): InputRecord

  override def flush(): Boolean = {
    producer.flush
    true
  }

  override def queueList(inputList: List[Mutation[_]]): Boolean = {
    inputList.foreach(i ⇒ producer.send(getTopic(i), getRecord(i)))
    true
  }

  override def queue(input: Mutation[_]): Boolean = {
    producer.send(getTopic(input), getRecord(input))
    true
  }

  override def toString(): String = {
    ""
  }

}

class KafkaMutationGenericAvroProducer(mappings: List[Mapping], config: Config)
    extends KafkaMutationAvroProducer(mappings, config) {

  override protected val schemaRepoClient = GenericInMemorySchemaRepo
  override protected val serializer = new AvroVersionedRecordSerializer[InputRecord](schemaRepoClient)

  override protected def getTopic(mutation: Mutation[_]): String = mutation.getClass match {
    case Insert ⇒ "insert"
    case Update ⇒ "update"
    case Delete ⇒ "delete"
  }

  override protected def getRecord(mutation: Mutation[_]): InputRecord = ???
}

class KafkaMutationSpecificAvroProducer(mappings: List[Mapping], config: Config)
    extends KafkaMutationAvroProducer(mappings, config) {

  private val schemaRepoClientClassName = config.getString("schema-repo-client")

  override protected val serializer = new AvroVersionedRecordSerializer[InputRecord](schemaRepoClient)
  override protected val schemaRepoClient = Class.forName(schemaRepoClientClassName)
    .newInstance()
    .asInstanceOf[GenericSchemaRepository[Short, Schema]]

  override protected def getTopic(mutation: Mutation[_]): String = ???
  override protected def getRecord(mutation: Mutation[_]): InputRecord = mutation.getClass match {
    // TODO: implement this properly
    case Insert ⇒ new mypipe.avro.InsertMutation()
    case Update ⇒ new mypipe.avro.UpdateMutation()
    case Delete ⇒ new mypipe.avro.DeleteMutation()
  }
}
