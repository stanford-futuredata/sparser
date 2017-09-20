package edu.stanford.sparser

import java.nio.{ByteBuffer, ByteOrder}

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.UnsafeRow

// TODO: change recordSize and maxRecords to match size of projected fields
class Sparser(val recordSize: Long = 4, val maxRecords: Long = 2^16) {

  val spNative = new SparserNative()
  var rawAddress: Long = 0L
  var recordsParsed: Long = 0L
  val buf = ByteBuffer.allocateDirect((maxRecords*recordSize).toInt)
  buf.order(ByteOrder.nativeOrder())

  def parseJson(jsonFilename: String, start: Long, length: Long, queryIndex: Int): Unit = {
    rawAddress = UnsafeAccess.getRawPointer(buf)
    println("In Scala, the address is " + "0x%08x".format(rawAddress))
    val before = System.currentTimeMillis()
    // invoke the native method
    recordsParsed = spNative.parse(jsonFilename, jsonFilename.length,
      rawAddress, start, length, queryIndex, recordSize, maxRecords)
    val timeMs = System.currentTimeMillis() - before
    println("spNative.parse: " + timeMs / 1000.0)
    println("In Scala, records parsed: " + recordsParsed)
  }

  def iterator(): Iterator[InternalRow] = {
    new Iterator[InternalRow]() {

      // TODO: change arg to UnsafeRow to match number of projected fields
      val currRecord = new UnsafeRow(1)
      var currRecordIndex: Long = 0

      override def hasNext(): Boolean = {
        currRecordIndex < recordsParsed
      }

      override def next(): UnsafeRow = {
        currRecord.pointTo(
          null,
          // TODO: figure out why it's 8 bytes ahead of where it should be
          rawAddress - 8 + currRecordIndex * recordSize,
          recordSize.toInt)
        currRecordIndex += 1
        val unsafeRowInt = currRecord.getInt(0)
        currRecord
      }
    }
  }
}
