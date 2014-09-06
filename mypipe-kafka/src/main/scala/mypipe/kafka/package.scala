package mypipe

package object kafka {

  /** Every message in Kafka has the following format:
   *
   *  -----------------
   *  | MAGIC | 1 byte  |
   *  |-----------------|
   *  | MTYPE | 1 byte  |
   *  |-----------------|
   *  | SCMID | N bytes |
   *  |-----------------|
   *  | DATA  | N bytes |
   *  -----------------
   *
   *  MAGIC: magic byte, used to figure out protocol version
   *  MTYPE: mutation type, a single byte indicating insert, update, or delete
   *  SCMID: Avro schema ID, variable number of bytes
   *  DATA: the actual mutation data as bytes, variable size
   */

  val PROTO_MAGIC_V0: Byte = 0x0.toByte
  val PROTO_HEADER_LEN_V0: Int = 2
}
