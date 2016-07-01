package mypipe.avro

import mypipe.api.event.Deserializer
import org.apache.avro.io.{DatumReader, BinaryDecoder, DecoderFactory}
import org.apache.avro.Schema

import scala.reflect.runtime.universe._
import org.apache.avro.specific.{SpecificDatumReader, SpecificRecord}
import java.util.logging.Logger

class AvroVersionedRecordDeserializer[InputRecord <: SpecificRecord](implicit val tag: TypeTag[InputRecord])
    extends Deserializer[Array[Byte], InputRecord, Schema] {

  protected val logger = Logger.getLogger(getClass.getName)
  lazy protected val inputRecordInstance: InputRecord = createInstance[InputRecord]
  lazy protected val readerSchema = inputRecordInstance.getSchema
  lazy protected val decoderFactory: DecoderFactory = DecoderFactory.get()
  lazy protected val decoder: BinaryDecoder = decoderFactory.binaryDecoder(Array[Byte](), null)
  lazy protected val reader: DatumReader[InputRecord] = new SpecificDatumReader[InputRecord](readerSchema)

  def createInstance[T: TypeTag]: T = {
    val tpe = typeOf[T]

      def fail = throw new IllegalArgumentException(s"Cannot instantiate $tpe")

    val noArgConstructor = tpe.member(nme.CONSTRUCTOR) match {
      case symbol: TermSymbol ⇒
        symbol.alternatives.collectFirst {
          case constr: MethodSymbol if constr.paramss == Nil || constr.paramss == List(Nil) ⇒ constr
        } getOrElse fail

      case NoSymbol ⇒ fail
    }
    val classMirror = typeTag[T].mirror.reflectClass(tpe.typeSymbol.asClass)
    classMirror.reflectConstructor(noArgConstructor).apply().asInstanceOf[T]
  }

  override def deserialize(schema: Schema, bytes: Array[Byte], offset: Int = 0): Option[InputRecord] = try {
    reader.setSchema(schema)
    val decodedData = decoderFactory.binaryDecoder(bytes, offset, bytes.length - offset, decoder)
    reader.read(inputRecordInstance, decodedData)
    Some(inputRecordInstance)
  } catch {
    case e: Exception ⇒
      logger.severe(s"Got an exception while trying to decode a versioned Avro event: ${e.getMessage}: schema=$schema bytes=${new String(bytes, "utf-8")}\n${e.getStackTraceString}")
      None
  }
}

