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
import java.util.UUID
import java.util.concurrent.ConcurrentHashMap

import scala.collection.JavaConversions.mapAsScalaConcurrentMap
import scala.collection.mutable.Map
import scala.compat.Platform
import scala.concurrent.Await
import scala.concurrent.duration.FiniteDuration

import org.apache.cassandra.io.sstable.SSTableReader

import com.tuplejump.snackfs.cassandra.partial.FileSystemStore
import com.tuplejump.snackfs.cassandra.sstable.DirectSSTableReader
import com.tuplejump.snackfs.cassandra.sstable.SubBlockData
import com.tuplejump.snackfs.fs.model.BlockMeta
import com.tuplejump.snackfs.util.AsyncUtil
import com.tuplejump.snackfs.util.LogConfiguration
import com.twitter.logging.Logger

case class BlockInputStream(store: FileSystemStore, blockMeta: BlockMeta, compressed: Boolean, keySpace: String, sstableLocation: String, isLocalBlock: Boolean, atMost: FiniteDuration, blockSize: Long, subBlockSize: Long) extends InputStream {

  private lazy val log = Logger.get(getClass)

  private val LENGTH = blockMeta.length
  
  private val blockToSSTableMap: Map[UUID, (String, SSTableReader)] = new ConcurrentHashMap[UUID, (String, SSTableReader)]()

  private var isClosed: Boolean = false
  private var inputStream: InputStream = null
  private var currentPosition: Long = 0
  private var targetSubBlockSize = 0L
  private var targetSubBlockOffset = 0L
  private var directSSTableReader: DirectSSTableReader = {
    if(isLocalBlock && store.getRemoteActor == null) {
      DirectSSTableReader(false, keySpace, sstableLocation)
    } else {
      null
    }
  }

  private def findSubBlock(targetPosition: Long): InputStream = {
    val subBlockLengthTotals = blockMeta.subBlocks.scanLeft(0L)(_ + _.length).tail
    val subBlockIndex = subBlockLengthTotals.indexWhere(p => targetPosition < p)
    if (subBlockIndex == -1) {
      val ex = new IOException("Impossible situation: could not find position " + targetPosition)
      log.error(ex, "Position %s could not be located", targetPosition.toString)
      throw ex
    }

    val start = Platform.currentTime
    var localFetch = true
    val subBlock = blockMeta.subBlocks(subBlockIndex)
    var inputStreamToReturn: InputStream = null
    if (LogConfiguration.isDebugEnabled) log.debug(Thread.currentThread.getName() + " Received request to retrieve blockID: %s and sublockID : %s index %s", blockMeta.id, subBlock.id, subBlockIndex)
    try {
      if (isLocalBlock) {
        try {
          var data: SubBlockData = null
          if(store.getRemoteActor != null) {
            inputStreamToReturn = store.getRemoteActor.readSSTable(blockMeta.id, subBlock.id, compressed)
          } else if (directSSTableReader != null) {
            inputStreamToReturn = {
              val data: SubBlockData = directSSTableReader.readSSTable(blockToSSTableMap, blockMeta.id, subBlock.id)
              if (data != null) {
                AsyncUtil.convertByteArrayToStream(data.bytes, compressed)
              }
              null
            }
          }
        } catch {
          case e: Exception =>
            log.error(e, "Failed to read sstable")
        }
      }
      if (!isLocalBlock || inputStreamToReturn == null) {
        localFetch = false
        inputStreamToReturn = Await.result(store.retrieveSubBlock(blockMeta.id, subBlock.id, compressed), atMost)
      }
      targetSubBlockSize = subBlock.length
      targetSubBlockOffset = subBlock.offset
      inputStreamToReturn
    } catch {
      case e: Exception =>
        log.error(e, "Failed to read inputStream")
        throw e
    } finally {
      log.info(Thread.currentThread.getName() + " Elapsed time to retrieve blockId: %s sblockId: %s is %s ms -> isLocal: %s LocalFetch: %s ", blockMeta.id, blockMeta.subBlocks(subBlockIndex).id, (Platform.currentTime - start), isLocalBlock, localFetch)
    }
  }

  def read: Int = {
    if (isClosed) {
      val ex = new IOException("Stream closed")
      log.error(ex, "Failed to read as stream is closed")
      throw ex
    }
    var result = -1
    var closeStream = false
    if (currentPosition <= LENGTH - 1) {
      if (currentPosition > (targetSubBlockOffset + targetSubBlockSize - 1) || closeStream) {
        if (inputStream != null) {
          inputStream.close()
          closeStream = false
        }
        inputStream = findSubBlock(currentPosition)
      }
      result = inputStream.read()
      if (result == -1) {
        closeStream = true;
      } else {
        currentPosition += 1
      }
    }
    result
  }

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
    var result = 0
    if (len > 0) {
      var closeStream = false
      while ((result < len) && (currentPosition <= LENGTH - 1)) {
        if (currentPosition > (targetSubBlockOffset + targetSubBlockSize - 1) || closeStream) {
          if (inputStream != null) {
            inputStream.close()
            closeStream = false
          }
          inputStream = findSubBlock(currentPosition)
        }
        val remaining = len - result
        val size = math.min(remaining, targetSubBlockSize)

        val readSize = inputStream.read(buf, off + result, size.asInstanceOf[Int])
        if (readSize == -1) {
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
      if (inputStream != null) {
        inputStream.close
        if(directSSTableReader != null) {
          directSSTableReader.close
        }
      }
      super.close
      isClosed = true
    }
  }
}
