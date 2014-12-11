package mypipe.kafka.consumer

import mypipe.api.event.Mutation
import mypipe.avro.GenericInMemorySchemaRepo
import mypipe.avro.schema.{ GenericSchemaRepository, AvroSchemaUtils }
import mypipe.kafka.KafkaGenericMutationAvroConsumer
import org.apache.avro.Schema

import scala.concurrent.duration._
import scala.concurrent.{ ExecutionContext, Await, Future }
import ExecutionContext.Implicits.global

class GenericConsoleConsumer(topic: String, zkConnect: String, groupId: String) {

  val timeout = 10 seconds
  var future: Option[Future[Unit]] = None

  val kafkaConsumer = new KafkaGenericMutationAvroConsumer[Short](
    topic = topic,
    zkConnect = zkConnect,
    groupId = groupId,
    schemaIdSizeInBytes = 2)(

    insertCallback = { insertMutation ⇒
      println(insertMutation)
      true
    },

    updateCallback = { updateMutation ⇒
      println(updateMutation)
      true
    },

    deleteCallback = { deleteMutation ⇒
      println(deleteMutation)
      true
    }) {

    protected val schemaRepoClient: GenericSchemaRepository[Short, Schema] = GenericInMemorySchemaRepo
    override def bytesToSchemaId(bytes: Array[Byte], offset: Int): Short = byteArray2Short(bytes, offset)
    private def byteArray2Short(data: Array[Byte], offset: Int) = (((data(offset) << 8)) | ((data(offset + 1) & 0xff))).toShort

    override protected def avroSchemaSubjectForMutationByte(byte: Byte): String = AvroSchemaUtils.genericSubject(Mutation.byteToString(byte))
  }

  def start(): Unit = {
    future = Some(kafkaConsumer.start)
  }

  def stop(): Unit = {

    kafkaConsumer.stop
    future.map(Await.result(_, timeout))
  }
}
