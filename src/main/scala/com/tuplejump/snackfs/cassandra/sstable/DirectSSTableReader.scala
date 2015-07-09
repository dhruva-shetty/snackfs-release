package com.tuplejump.snackfs.cassandra.sstable

import java.nio.file.Files
import java.nio.file.Paths
import java.util.UUID
import java.util.concurrent.ConcurrentHashMap
import scala.collection.JavaConversions.asScalaBuffer
import scala.collection.JavaConversions.mapAsScalaConcurrentMap
import scala.collection.mutable.Map
import scala.compat.Platform
import org.apache.cassandra.config.CFMetaData
import org.apache.cassandra.db.AtomDeserializer
import org.apache.cassandra.db.Cell
import org.apache.cassandra.db.ColumnSerializer
import org.apache.cassandra.db.DecoratedKey
import org.apache.cassandra.db.DeletionInfo
import org.apache.cassandra.db.DeletionTime
import org.apache.cassandra.db.RowIndexEntry
import org.apache.cassandra.db.composites.SimpleDenseCellNameType
import org.apache.cassandra.db.marshal.BytesType
import org.apache.cassandra.io.sstable.SSTableReader
import org.apache.cassandra.io.util.FileUtils
import org.apache.cassandra.io.util.RandomAccessReader
import org.apache.cassandra.service.StorageService
import org.apache.cassandra.utils.ByteBufferUtil
import org.apache.commons.lang.StringUtils
import com.tuplejump.snackfs.SnackFSMode.AKKA
import com.tuplejump.snackfs.SnackFSMode.LOCAL
import com.tuplejump.snackfs.SnackFSMode.NETTY
import com.tuplejump.snackfs.SnackFSMode.SnackFSMode
import com.tuplejump.snackfs.SnackFSMode.isLocal
import com.tuplejump.snackfs.util.LogConfiguration
import com.twitter.logging.Logger
import io.netty.channel.DefaultFileRegion
import java.io.IOException

case class SubBlockData(fileName:String, positionToSeek: Long, length: Int, bytes: Array[Byte], fileRegion: DefaultFileRegion) extends Serializable

case class DirectSSTableReader(mode: SnackFSMode, keyspace: String, sstableLocation: String) {

  private lazy val log = Logger.get(getClass)
  
  private val SBLOCK_COLUMN_FAMILY = "sblock"

  private val DATA_FILES_PATH = sstableLocation + keyspace
  
  private val COLUMN_FAMILY_METADATA = SSTableStore.getMetaData(keyspace, SBLOCK_COLUMN_FAMILY)
  
  private val sstableToFileMap: Map[String, RandomAccessReader] = new ConcurrentHashMap[String, RandomAccessReader]()
  
  private var ssTables: Map[String, SSTableReader] = null
  
  private var initialized: Boolean = false
  
  def initialize {
     ssTables = SSTableStore.getSSTables(keyspace, DATA_FILES_PATH, SBLOCK_COLUMN_FAMILY)
     initialized = true;
  }
  
  def readSSTable(blockToSSTableMap: Map[UUID, (String, SSTableReader)], blockUUId: UUID, subBlockUUId: UUID): SubBlockData = {
    if(!initialized) {
      initialize
    }
    val decoratedKey = StorageService.getPartitioner().decorateKey(ByteBufferUtil.bytes(blockUUId));
    try {
      val cachedEntry = blockToSSTableMap.get(blockUUId)
      if (blockToSSTableMap.contains(blockUUId) && Files.exists(Paths.get(cachedEntry.head._1))) {
        val sstable = cachedEntry.head._2
        if (sstable.getBloomFilter.isPresent(decoratedKey.getKey)) {
          return getData(sstable, decoratedKey, blockUUId, subBlockUUId)
        }
      }
      if (ssTables != null) {
        ssTables.filter(sstable => sstable._2.getBloomFilter.isPresent(decoratedKey.getKey)).foreach { sstableEntry =>
          val data =  getData(sstableEntry._2, decoratedKey, blockUUId, subBlockUUId)
          if (data != null) {
            return data
          }
        }
      }
    } catch {
      case e: Exception =>
        log.error(e, "Failed to read sstable")
    }
    log.info(Thread.currentThread.getName() + " blockID: %s and sblockID: %s not found in sstable", blockUUId, subBlockUUId)
    null
  }

  def getData(sstable: SSTableReader, decoratedKey: DecoratedKey, blockUUId: UUID, subBlockUUId: UUID): SubBlockData = {
    var dfile:RandomAccessReader = null
    if(isLocal(mode) && sstableToFileMap.contains(sstable.getFilename)) {
      dfile = sstableToFileMap.get(sstable.getFilename).head
    } else {
      dfile = sstable.openDataReader
      sstableToFileMap.put(sstable.getFilename, dfile)
    }
    val row = sstable.getPosition(decoratedKey, SSTableReader.Operator.EQ, false);
    if (row != null && row.position != 0) retrieveColumn(sstable.getFilename, blockUUId, subBlockUUId, dfile, row, sstable) else null
  }

  def retrieveColumn(fileName: String, blockUUId: UUID, subBlockUUId: UUID, dfile: RandomAccessReader, row: RowIndexEntry, sstable: SSTableReader): SubBlockData = {
    dfile.seek(row.position)
    ByteBufferUtil.readWithShortLength(dfile)
    if (new DeletionInfo(DeletionTime.serializer.deserialize(dfile)).isLive) {
      val sBlockUUID = subBlockUUId.toString.replaceAll("-", StringUtils.EMPTY)
      row.columnsIndex.foreach { indexInfo =>
        {
          if (COLUMN_FAMILY_METADATA.comparator.getString(indexInfo.firstName).contains(sBlockUUID)) {
        	  var positionToSeek = row.position + indexInfo.offset;
            dfile.seek(positionToSeek)
            var mark = dfile.mark();
            if(dfile.bytesPastMark(mark) < indexInfo.width) {
              var sblockLength = -1
              mode match {
                case LOCAL =>
                  return SubBlockData(fileName, -1, sblockLength, loadColumn(sstable.metadata.getOnDiskDeserializer(dfile, sstable.descriptor.version), sstable.metadata, blockUUId, subBlockUUId), null)

                case NETTY =>
                  val start = Platform.currentTime
                  try {
                    while(dfile.bytesPastMark(mark) < indexInfo.width) {
                      sblockLength = getSBlockLength(dfile, sBlockUUID)
                      if(sblockLength >= 0) {
                    	  val position = positionToSeek + dfile.bytesPastMark(mark)
                        return SubBlockData(fileName, position, sblockLength, null, new DefaultFileRegion(dfile.getChannel, position, sblockLength))
                      }
                    }
                  } finally {
                    log.info(Thread.currentThread.getName() + " Elapsed time to load-column-length %s ms, length %s", (Platform.currentTime - start), sblockLength)
                  }
                  
                case AKKA =>
                  val start = Platform.currentTime
                  try {
                    while(dfile.bytesPastMark(mark) < indexInfo.width) {
                      sblockLength = getSBlockLength(dfile, sBlockUUID)
                      if(sblockLength >= 0) {
                        val position = positionToSeek + dfile.bytesPastMark(mark)
                        return SubBlockData(fileName, position, sblockLength, null, null)
                      }
                    }
                  } finally {
                    dfile.close
                    log.info(Thread.currentThread.getName() + " Elapsed time to load-column-length %s ms, length %s", (Platform.currentTime - start), sblockLength)
                  }
              }
            }
          }
        }
      }
    }
    null
  }
  
  def getSBlockLength(dfile: RandomAccessReader, subBlockUUId: String): Int = {
    val name:String = COLUMN_FAMILY_METADATA.comparator.getString(new SimpleDenseCellNameType(BytesType.instance).fromByteBuffer(ByteBufferUtil.readWithShortLength(dfile)))
    val mask = dfile.readUnsignedByte
    val timestamp = dfile.readLong
    val sblockLength: Int = dfile.readInt
    if (sblockLength < 0) {
        throw new IOException("Corrupt (negative) value length encountered");
    }
    if(sblockLength == 0 || !name.contains(subBlockUUId) || (mask & ColumnSerializer.RANGE_TOMBSTONE_MASK) != 0 || (mask & ColumnSerializer.DELETION_MASK) != 0 || (mask & ColumnSerializer.EXPIRATION_MASK) != 0) {
      FileUtils.skipBytesFully(dfile, sblockLength)
      return -1
    } else {
      return sblockLength
    }
  }

  def loadColumn(deserializer: AtomDeserializer, metadata: CFMetaData, blockUUId: UUID, subBlockUUId: UUID): Array[Byte] = {
    val start = Platform.currentTime
    deserializer.skipNext()
    if (deserializer.hasNext) {
      val ioStart = Platform.currentTime
      val cell = deserializer.readNext.asInstanceOf[Cell]
      val ioEnd = Platform.currentTime - ioStart
      if (cell.value.array.length != 0) {
        try {
          return cell.value.array
        } finally {
          log.info(Thread.currentThread.getName() + " Elapsed time to load-column %s ms, io-time %s ms, length %s", (Platform.currentTime - start), ioEnd, cell.value.array.length)
        }
      }
      if (LogConfiguration.isDebugEnabled) log.debug(Thread.currentThread.getName() + " sblock %s, cell %s, size %s", subBlockUUId, metadata.comparator.getString(cell.name), cell.value.array.length)
    }
    log.info(Thread.currentThread.getName() + " Cell not found for blockID: %s and sblockID: %s ", blockUUId, subBlockUUId)
    null
  }
  
  def close = {
    sstableToFileMap.values.foreach { dfile => dfile.close }
    sstableToFileMap.clear
  }
}