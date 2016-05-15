package mypipe.kafka.consumer

import kafka.serializer._
import org.apache.avro.specific.SpecificRecord
import org.slf4j.LoggerFactory

import scala.reflect.runtime._
import scala.reflect.runtime.universe._

class KafkaMutationAvroConsumer[InsertMutationType <: SpecificRecord, UpdateMutationType <: SpecificRecord, DeleteMutationType <: SpecificRecord](
  topic: String,
  zkConnect: String,
  groupId: String,
  valueDecoder: Decoder[SpecificRecord])(insertCallback: (InsertMutationType) ⇒ Boolean,
                                         updateCallback: (UpdateMutationType) ⇒ Boolean,
                                         deleteCallback: (DeleteMutationType) ⇒ Boolean,
                                         implicit val insertTag: TypeTag[InsertMutationType],
                                         implicit val updateTag: TypeTag[UpdateMutationType],
                                         implicit val deleteTag: TypeTag[DeleteMutationType])
    extends KafkaConsumer[SpecificRecord](topic, zkConnect, groupId, valueDecoder) {

  protected val logger = LoggerFactory.getLogger(getClass.getName)

  protected val InsertClass = currentMirror.runtimeClass(insertTag.tpe.typeSymbol.asClass)
  protected val UpdateClass = currentMirror.runtimeClass(updateTag.tpe.typeSymbol.asClass)
  protected val DeleteClass = currentMirror.runtimeClass(deleteTag.tpe.typeSymbol.asClass)

  override def onEvent(mutation: SpecificRecord): Boolean = try {
    mutation.getClass match {
      case InsertClass ⇒ insertCallback(mutation.asInstanceOf[InsertMutationType])
      case UpdateClass ⇒ updateCallback(mutation.asInstanceOf[UpdateMutationType])
      case DeleteClass ⇒ deleteCallback(mutation.asInstanceOf[DeleteMutationType])
      case _           ⇒ false
    }
  } catch {
    case e: Exception ⇒
      log.error("Could not run callback on {} => {}: {}", mutation, e.getMessage, e.getStackTrace.mkString(sys.props("line.separator")))
      false
  }
}

