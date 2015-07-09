package com.tuplejump.snackfs.server.messages

import java.nio.channels.WritableByteChannel

import io.netty.buffer.ByteBuf
import io.netty.channel.FileRegion
import io.netty.util.AbstractReferenceCounted
import io.netty.util.ReferenceCountUtil

class MessageWithHeader(val header: ByteBuf, val body: FileRegion, private val bodyLength: Long) extends AbstractReferenceCounted with FileRegion {
  
  private val headerLength = header.readableBytes

  private var totalBytesTransferred: Long = _

  override def count(): Long = headerLength + bodyLength

  override def position(): Long = 0

  override def transfered(): Long = totalBytesTransferred

  override def transferTo(target: WritableByteChannel, position: Long): Long = {
    var writtenHeader = 0L
    if (header.readableBytes() > 0) {
      writtenHeader = copyByteBuf(header, target)
      totalBytesTransferred += writtenHeader
      if (header.readableBytes() > 0) {
        return writtenHeader
      }
    }
    val writtenBody = body.transferTo(target, totalBytesTransferred - headerLength)
    totalBytesTransferred += writtenBody
    writtenHeader + writtenBody
  }

  protected override def deallocate() {
    try {
      header.release()
      ReferenceCountUtil.release(body)
    } catch {
      case ignore: Exception => 
    }
  }

  private def copyByteBuf(buf: ByteBuf, target: WritableByteChannel): Int = {
    val written = target.write(buf.nioBuffer())
    buf.skipBytes(written)
    written
  }
}