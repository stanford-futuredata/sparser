package edu.stanford.sparser

import java.nio.{ByteBuffer, ByteOrder}

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.UnsafeRow

class Sparser(val numFields: Int = 1, val recordSizeInBytes: Long = 16, val maxRecords: Long = 16777216) {

  val spNative = new SparserNative()
  var rawAddress: Long = 0L
  var recordsParsed: Long = 0L
  val buf: ByteBuffer = ByteBuffer.allocateDirect((maxRecords*recordSizeInBytes).toInt)
  buf.order(ByteOrder.nativeOrder())

  def parseJson(jsonFilename: String, start: Long, length: Long, queryIndex: Int): Unit = {
    rawAddress = UnsafeAccess.getRawPointer(buf)
    println("In Scala, the address is " + "0x%08x".format(rawAddress))
    // invoke the native method
    recordsParsed = spNative.parse(jsonFilename, jsonFilename.length,
      rawAddress, start, length, queryIndex, maxRecords)
    println("In Scala, records parsed: " + recordsParsed)
  }

  def iterator(): Iterator[InternalRow] = {
    new Iterator[InternalRow]() {

      val currRecord = new UnsafeRow(numFields)
      var currRecordIndex: Long = 0

      override def hasNext(): Boolean = {
        currRecordIndex < recordsParsed
      }

      override def next(): UnsafeRow = {
        currRecord.pointTo(
          null,
          rawAddress + currRecordIndex * recordSizeInBytes,
          recordSizeInBytes.toInt)
        currRecordIndex += 1
        currRecord
      }
    }
  }
}
