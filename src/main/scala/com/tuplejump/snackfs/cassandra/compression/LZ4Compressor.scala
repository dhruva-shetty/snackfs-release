package com.tuplejump.snackfs.cassandra.compression

import org.jboss.netty.buffer.ChannelBuffers

import com.tuplejump.snackfs.util.LogConfiguration
import com.twitter.logging.Logger

import net.jpountz.lz4.LZ4Factory

object LZ4CompressorInstance {
  
  private lazy val log = Logger.get(getClass)  
  
  val lz4Factory = LZ4Factory.fastestInstance();
  val compressor = lz4Factory.fastCompressor();
  val decompressor = lz4Factory.fastDecompressor();
  val INTEGER_BYTES: Int = 4
  
  def compress(input: Array[Byte], compressed: Boolean): Array[Byte] = {
    if(compressed) {
      val maxCompressedLength = compressor.maxCompressedLength(input.length)
      val output = Array.ofDim[Byte](INTEGER_BYTES + maxCompressedLength)
      output(0) = (input.length >>> 24).toByte
      output(1) = (input.length >>> 16).toByte
      output(2) = (input.length >>> 8).toByte
      output(3) = (input.length).toByte
      val written = compressor.compress(input, 0, input.length, output, INTEGER_BYTES, maxCompressedLength)
      ChannelBuffers.wrappedBuffer(output, 0, INTEGER_BYTES + written).array
    } else {
      input
    }
  }
  
  def decompress(input: Array[Byte], compressed: Boolean): Array[Byte] = {
    if(compressed) {
      val uncompressedLength = ((input(0) & 0xFF) << 24) | ((input(1) & 0xFF) << 16) | ((input(2) & 0xFF) << 8) | ((input(3) & 0xFF))
      val output = Array.ofDim[Byte](uncompressedLength)
      val read = decompressor.decompress(input, INTEGER_BYTES, output, 0, uncompressedLength)
      ChannelBuffers.wrappedBuffer(output).array
    } else {
      input
    }
  }
}

