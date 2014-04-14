package mypipe.avro

import mypipe.api._
import org.apache.avro.io.{ DatumReader, BinaryDecoder, DecoderFactory }
import org.apache.avro.Schema

import scala.reflect.runtime.universe
import scala.reflect.runtime.universe.TypeTag
import org.apache.avro.specific.{ SpecificDatumReader, SpecificRecord }
import java.util.logging.Logger

class AvroVersionedRecordDeserializer[InputRecord <: SpecificRecord]()(implicit tag: TypeTag[InputRecord])
    extends Deserializer[Array[Byte], InputRecord, Schema] {

  protected val logger = Logger.getLogger(getClass.getName)
  lazy protected val inputRecordInstance: InputRecord = getInstanceByReflection[InputRecord]
  lazy protected val readerSchema = inputRecordInstance.getSchema
  lazy protected val decoderFactory: DecoderFactory = DecoderFactory.get()
  lazy protected val decoder: BinaryDecoder = decoderFactory.binaryDecoder(Array[Byte](), null)
  lazy protected val reader: DatumReader[InputRecord] = new SpecificDatumReader[InputRecord](readerSchema)

  protected def getInstanceByReflection[InstanceType](implicit instanceTypeTag: TypeTag[InstanceType]): InstanceType = {

    val runtimeMirror = universe.runtimeMirror(getClass.getClassLoader)
    val inputRecordClass = universe.typeOf[InstanceType].typeSymbol.asClass
    val inputRecordClassMirror = runtimeMirror.reflectClass(inputRecordClass)
    val inputRecordConstructor = universe.typeOf[InstanceType].declaration(universe.nme.CONSTRUCTOR).asTerm.alternatives.head.asMethod
    val inputRecordConstructorMirror = inputRecordClassMirror.reflectConstructor(inputRecordConstructor)
    inputRecordConstructorMirror().asInstanceOf[InstanceType]
  }

  override def deserialize(schema: Schema, bytes: Array[Byte], offset: Int = 0): Option[InputRecord] = try {
    reader.setSchema(schema)
    val decodedData = decoderFactory.binaryDecoder(bytes, offset, bytes.length - offset, decoder)
    reader.read(inputRecordInstance, decodedData)
    Some(inputRecordInstance)
  } catch {
    case e: Exception ⇒
      logger.severe(s"Got an exception while trying to decode a versioned avro event! Exception: ${e.getMessage}\n${e.getStackTraceString}")
      None
  }
}

