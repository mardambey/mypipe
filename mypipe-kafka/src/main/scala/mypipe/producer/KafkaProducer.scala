package mypipe.producer

import mypipe.api._
import mypipe.kafka.{ KafkaProducer ⇒ KProducer, KafkaAvroVersionedProducer }
import com.typesafe.config.Config
import mypipe.avro.schema.SchemaRepo
import org.apache.avro.specific.SpecificRecord

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

class KafkaMutationAvroProducer(mappings: List[Mapping], config: Config)
    extends Producer(mappings = null, config = config) {

  type InputRecord = SpecificRecord

  protected val schemaRepoClient = SchemaRepo
  protected val producer = new KafkaAvroVersionedProducer[InputRecord](metadataBrokers, schemaRepoClient)
  protected val metadataBrokers = config.getString("metadata-brokers")

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

  protected def getTopic(mutation: Mutation[_]): String = ???
  protected def getRecord(mutation: Mutation[_]): InputRecord = ???
}