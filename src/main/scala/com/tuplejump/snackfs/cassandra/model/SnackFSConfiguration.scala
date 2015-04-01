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

package com.tuplejump.snackfs.cassandra.model

import java.util.ArrayList
import java.util.HashSet
import java.util.List
import java.util.Set
import scala.collection.JavaConversions.asScalaBuffer
import scala.collection.JavaConversions.asScalaSet
import scala.concurrent.duration.FiniteDuration
import scala.concurrent.duration.MILLISECONDS
import scala.util.Random
import org.apache.cassandra.locator.SimpleStrategy
import org.apache.cassandra.thrift.ConsistencyLevel
import org.apache.commons.lang.StringUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import com.tuplejump.snackfs.util.LogConfiguration
import com.tuplejump.snackfs.util.NetworkHostUtil
import com.twitter.logging.Logger
import java.net.InetAddress

case class SnackFSConfiguration(CassandraHost: String, 
                                CassandraThriftPort: Int,
                                CassandraCqlPort: Int,
                                readConsistencyLevel: ConsistencyLevel, 
                                writeConsistencyLevel: ConsistencyLevel,
                                keySpace: String, 
                                blockSize: Long, 
                                subBlockSize: Long, 
                                atMost: FiniteDuration,
                                queryDuration: FiniteDuration, 
                                replicationFactor: Int, 
                                replicationStrategy: String, 
                                useSSTable: Boolean, 
                                sstableLocation: String, 
                                useLocking: Boolean,
                                uncompressedFileFormats: Set[String],
                                useACL: Boolean,
                                standalone: Boolean) {
  
  private lazy val log = Logger.get(getClass)
  
  def isCompressed(path: Path): Boolean = {
	  var compressed = true
    if (uncompressedFileFormats.size == 0) {
      compressed
    } else {
      val fileParts: Array[String] = StringUtils.split(path.getName, ".")
      if (fileParts.length != 0) {
        val fileFormat = fileParts(fileParts.length - 1)
        if(uncompressedFileFormats.contains(fileFormat)) {
          compressed = false
        }
        if (compressed) {
          uncompressedFileFormats.foreach { format: String =>
            {
              if (path.toString.contains(format)) {
                compressed = false
              }
            }
          }
        }
      }
      
      if(LogConfiguration.isDebugEnabled()) log.debug(Thread.currentThread.getName + " path: %s compressed: %s", path, compressed)
      compressed
    }
  }
}

object SnackFSConfiguration {

  private val CONSISTENCY_LEVEL = ConsistencyLevel.LOCAL_QUORUM
  private val REPLICATION_STRATEGY = classOf[SimpleStrategy].getCanonicalName
  private val KEYSPACE = "snackfs"
  private val SSTABLE = "/local2/data/cassandra/data/"
  private val LOCAL_HOST = NetworkHostUtil.getHostAddress
  private val THRIFT_PORT: Int = 9160
  private val CQl_PORT: Int = 9042
  private val AT_MOST: Long = 10 * 1000 //10 seconds
  private val QUERY_WAIT_TIMEOUT: Long = 15 * 60 * 1000 //15 minutes
  private val SUB_BLOCK_SIZE: Long =  8 * 1024 * 1024 //8 MB
  private val BLOCK_SIZE: Long = 128 * 1024 * 1024 //128MB
  private val REPLICATION_FACTOR: Int = 3

  def get(userConf: Configuration): SnackFSConfiguration = {
    val thriftPort = userConf.getInt("snackfs.cassandra.thrift.port", THRIFT_PORT)
    val cqlPort = userConf.getInt("snackfs.cassandra.cql.port", CQl_PORT)
    val writeLevel = getConsistencyLevel(userConf.get("snackfs.consistencyLevel.write"))
    val consistencyLevelRead = userConf.get("snackfs.consistencyLevel.read")
    val readLevel = getConsistencyLevel(consistencyLevelRead)
    val keyspace: String = optIfNull(userConf.get("snackfs.keyspace"), KEYSPACE)
    val sstableLocation: String = optIfNull(userConf.get("sstable.location"), SSTABLE)
    val useLocking = userConf.get("snackfs.filelock", "true").toBoolean
    val replicationFactor = userConf.getInt("snackfs.replicationFactor", REPLICATION_FACTOR)
    val replicationStrategy = optIfNull(userConf.get("snackfs.replicationStrategy"), REPLICATION_STRATEGY)
    val subBlockSize = userConf.getLong("snackfs.subblock.size", SUB_BLOCK_SIZE)
    val blockSize = userConf.getLong("snackfs.block.size", BLOCK_SIZE)
    val waitDuration = FiniteDuration(userConf.getLong("snackfs.waitInterval", AT_MOST), MILLISECONDS)
    val queryDuration = FiniteDuration(userConf.getLong("snackfs.queryWaitInterval", QUERY_WAIT_TIMEOUT), MILLISECONDS)
    val useSSTable = userConf.get("snackfs.useSSTable", "false").toBoolean
    val uncompressedFileFormats: Set[String] = new HashSet[String]()
    val unCompressedFormats = userConf.get("snackfs.fileformats.uncompressed")
    if(!StringUtils.isEmpty(unCompressedFormats)) {
      StringUtils.split(unCompressedFormats, ",").foreach { format => uncompressedFileFormats.add(format) }
    }
    
    val useACL = userConf.get("acls.enabled", "true").toBoolean
    val standalone = userConf.get("standalone.mode", "true").toBoolean
    val cassandraHostAddresses = userConf.get("snackfs.cassandra.hosts")
    val cassandraHosts: List[String] = new ArrayList[String]()
    if(!StringUtils.isEmpty(cassandraHostAddresses)) {
      StringUtils.split(cassandraHostAddresses, ",").foreach { host => cassandraHosts.add(InetAddress.getByName(host).getHostAddress) }
    }
    var localhost = false
    cassandraHosts.foreach { host => 
      if(! localhost && LOCAL_HOST.equals(host)) {
        localhost = true
      }
    }
    val host = if(localhost || cassandraHosts.size == 0) LOCAL_HOST else cassandraHosts.get(Random.nextInt(cassandraHosts.size))
    SnackFSConfiguration(host, thriftPort, cqlPort, readLevel, writeLevel, keyspace, blockSize, subBlockSize, waitDuration, queryDuration, replicationFactor, replicationStrategy, useSSTable && localhost, sstableLocation, useLocking, uncompressedFileFormats, useACL, standalone)
  }

  private def getConsistencyLevel(level: String): ConsistencyLevel = {
    if (level != null) {
      ConsistencyLevel.valueOf(level)
    } else {
      CONSISTENCY_LEVEL
    }
  }

  private def optIfNull(valueToCheck: String, alternativeOption: String): String = {
    if (valueToCheck == null) {
      alternativeOption
    } else {
      valueToCheck
    }
  }

}
