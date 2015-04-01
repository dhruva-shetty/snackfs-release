/*
 * Licensed to Tuplejump Software Pvt. Ltd. under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  Tuplejump Software Pvt. Ltd. licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */
package com.tuplejump.snackfs.fs.stream

import java.io.IOException
import java.io.InputStream

import scala.concurrent.Await
import scala.concurrent.duration.DurationInt

import org.apache.hadoop.fs.FSInputStream
import org.apache.hadoop.fs.Path

import com.tuplejump.snackfs.cassandra.partial.FileSystemStore
import com.tuplejump.snackfs.util.LogConfiguration
import com.tuplejump.snackfs.util.NetworkHostUtil
import com.twitter.logging.Logger

case class FileSystemInputStream(store: FileSystemStore, path: Path) extends FSInputStream {

  private lazy val log = Logger.get(getClass)

  private val inodeFuture = store.retrieveINode(path)
  private val blockLocationFuture = store.getBlockLocations(path)
  private val INODE = Await.result(inodeFuture, 30 seconds)
  private val FILE_LENGTH: Long = INODE.blocks.map(_.length).sum
  private val BLOCK_LOCATIONS = Await.result(blockLocationFuture, 30 seconds)
  private val compressed = store.config.isCompressed(path)

  private var blockStream: InputStream = null
  private var currentPosition: Long = 0L
  private var currentBlockSize: Long = -1
  private var currentBlockOffset: Long = 0
  private var isClosed: Boolean = false

  def seek(target: Long) = {
    if (target > FILE_LENGTH) {
      val ex = new IOException("Cannot seek after EOF: " + path + " length: " + FILE_LENGTH + " target: " + target)
      log.error(ex, "EOF reached earlier: " + path + " length: " + FILE_LENGTH + " target: " + target)
      throw ex
    }
    currentPosition = target
    currentBlockSize = -1
    currentBlockOffset = 0
  }

  def getPos: Long = currentPosition

  def seekToNewSource(targetPos: Long): Boolean = false

  private def findBlock(targetPosition: Long): InputStream = {
    val blockIndex = INODE.blocks.indexWhere(b => b.offset + b.length > targetPosition)
    if (blockIndex == -1) {
      val ex = new IOException("Impossible situation: could not find position " + targetPosition + " file length " + FILE_LENGTH)
      log.error(ex, "Position %s could not be located, file length %s", targetPosition.toString, FILE_LENGTH)
      throw ex
    }
    
    val block = INODE.blocks(blockIndex)
    currentBlockSize = block.length
    currentBlockOffset = block.offset

    val offset = targetPosition - currentBlockOffset
    val localHostLocation = NetworkHostUtil.getHostAddress
    var isLocalBlock = false
    for (hostLocations <- BLOCK_LOCATIONS.get(block) if !isLocalBlock) {
      for(hostLocation <- hostLocations if (hostLocation.equals(localHostLocation) && !isLocalBlock) ) {
        if(LogConfiguration.isDebugEnabled) log.debug(Thread.currentThread.getName() + " Block location identified to be local - will be using sstables on %s", localHostLocation)
        isLocalBlock = true
      }
    }
    if(!isLocalBlock) {
      if(LogConfiguration.isDebugEnabled) log.debug(Thread.currentThread.getName() + " Localhost %s, Block ID %s, BlockHosts %s", localHostLocation, block.id, BLOCK_LOCATIONS.get(block))
    }
    try {
      val bis = store.retrieveBlock(block, compressed, isLocalBlock)
      bis.skip(offset)
      bis
    } catch {
      case e: Exception =>
        log.error(e, "Failed to retrieve file %s", path)
        throw e
    }
  }

  def read(): Int = {
    if (isClosed) {
      val ex = new IOException("Stream closed")
      log.error(ex, "Failed to read as stream is closed")
      throw ex
    }
    var result: Int = -1
    var closeStream = false
    if (currentPosition < FILE_LENGTH) {
      if ((currentPosition > currentBlockOffset + currentBlockSize) || closeStream) {
        if (blockStream != null) {
          blockStream.close()
          closeStream = false
        }
        blockStream = findBlock(currentPosition)
      }
      result = blockStream.read
      if(result == -1) {
          closeStream = true;
      } else {
        currentPosition += 1
      }
    }
    result
  }

  override def available: Int = (FILE_LENGTH - currentPosition).asInstanceOf[Int]

  override def read(buf: Array[Byte], off: Int, len: Int): Int = {
    if (isClosed) {
      val ex = new IOException("Stream closed")
      log.error(ex, "Failed to read as stream is closed")
      throw ex
    }
    if (buf == null) {
      val ex = new NullPointerException
      log.error(ex, "Failed to read as output buffer is null")
      throw ex
    }
    if ((off < 0) || (len < 0) || (len > buf.length - off)) {
      val ex = new IndexOutOfBoundsException
      log.error(ex, "Failed to read as one of offset,length or output buffer length is invalid")
      throw ex
    }

    var result: Int = 0
    var closeStream = false
    if (len > 0) {
      while ((result < len) && (currentPosition <= FILE_LENGTH - 1) || closeStream) {
        if (currentPosition > currentBlockOffset + currentBlockSize - 1) {
          if (blockStream != null) {
            blockStream.close()
            closeStream = false
          }
          blockStream = findBlock(currentPosition)
        }
        val realLen: Int = math.min(len - result, currentBlockSize + 1).asInstanceOf[Int]
        var readSize = blockStream.read(buf, off + result, realLen)
        if(readSize == -1) {
          closeStream = true;
        } else {
          result += readSize
          currentPosition += readSize
        }
      }
      if (result == 0) {
        result = -1
      }
    }
    result
  }

  override def close() = {
    if (!isClosed) {
      if (blockStream != null) {
        blockStream.close
      }
      super.close
      isClosed = true
    }
  }
}
