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

import java.net.InetAddress
import java.util.ArrayList
import java.util.List
import java.util.Set
import scala.collection.JavaConversions.asScalaBuffer
import scala.collection.JavaConversions.asScalaSet
import scala.collection.JavaConversions.setAsJavaSet
import scala.concurrent.duration.FiniteDuration
import scala.concurrent.duration.MILLISECONDS
import scala.util.Random
import org.apache.cassandra.thrift.ConsistencyLevel
import org.apache.commons.lang.StringUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import com.tuplejump.snackfs.util.LogConfiguration
import com.tuplejump.snackfs.util.NetworkHostUtil
import com.twitter.logging.Logger
import org.apache.cassandra.dht.IPartitioner
import org.apache.cassandra.dht.LocalPartitioner
import org.apache.cassandra.utils.FBUtilities
import java.util.Collections

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
                                replicationDataCenter: String,
                                replicationStrategy: String, 
                                useSSTable: Boolean, 
                                sstableLocation: String, 
                                partitioner: String,
                                useLocking: Boolean,
                                uncompressedFileFormats: Set[String],
                                useACL: Boolean,
                                clientMode: String,
                                snackFSServerPort: Int,
                                snackFSClientTimeout: Int) {
  
  private lazy val log = Logger.get(getClass)
  
  private val cassandraPartitioner: IPartitioner = FBUtilities.newPartitioner(partitioner)
  
  def isCompressed(path: Path): Boolean = {
	  var compressed = true
    if (uncompressedFileFormats.size == 0) {
      compressed
    } else {
      val fileParts: Array[String] = StringUtils.split(path.getName, ".")
      if (fileParts.length != 0) {
        val fileFormat = fileParts(fileParts.length - 1)
        compressed = !uncompressedFileFormats.contains(fileFormat)
        if (compressed) {
          uncompressedFileFormats.foreach { format: String => if (path.toString.contains(format)) compressed = false }
        }
      }
      if(LogConfiguration.isDebugEnabled()) log.debug(Thread.currentThread.getName + " path: %s compressed: %s", path, compressed)
      compressed
    }
  }
  
  def getPartitioner: IPartitioner = cassandraPartitioner
}

object SnackFSConfiguration {

  private val CONSISTENCY_LEVEL = ConsistencyLevel.LOCAL_QUORUM
  private val REPLICATION_STRATEGY = "SimpleStrategy"
  private val REPLICATION_FACTOR: Int = 3
  private val KEYSPACE = "snackfs"

  private val LOCAL_HOST = NetworkHostUtil.getHostAddress
  private val SNACKFS_SERVER_PORT: Int = 55252
  private val THRIFT_PORT: Int = 9160
  private val CQl_PORT: Int = 9042
  private val AT_MOST: Long = 10 * 1000 //10 seconds
  
  private val QUERY_WAIT_TIMEOUT: Long = 15 * 60 * 1000 //15 minutes
  private val SUB_BLOCK_SIZE: Long =  8 * 1024 * 1024 //8 MB
  private val BLOCK_SIZE: Long = 128 * 1024 * 1024 //128MB

  def get(userConf: Configuration): SnackFSConfiguration = {
    val thriftPort = userConf.getInt("snackfs.cassandra.thrift.port", THRIFT_PORT)
    val cqlPort = userConf.getInt("snackfs.cassandra.cql.port", CQl_PORT)
    val writeLevel = getConsistencyLevel(userConf.get("snackfs.consistencyLevel.write"))
    val consistencyLevelRead = userConf.get("snackfs.consistencyLevel.read")
    val readLevel = getConsistencyLevel(consistencyLevelRead)
    val keyspace: String = optIfNull(userConf.get("snackfs.keyspace"), KEYSPACE)
    val sstableLocation: String = userConf.get("sstable.location")
    val partitioner: String = userConf.get("cassandra.partitioner")
    val useLocking = userConf.get("snackfs.filelock", "true").toBoolean
    val replicationFactor = userConf.getInt("snackfs.replicationFactor", REPLICATION_FACTOR)
    val replicationDataCenter = userConf.get("snackfs.replicationDataCenter")
    val replicationStrategy = optIfNull(userConf.get("snackfs.replicationStrategy"), REPLICATION_STRATEGY)
    val subBlockSize = userConf.getLong("snackfs.subblock.size", SUB_BLOCK_SIZE)
    val blockSize = userConf.getLong("snackfs.block.size", BLOCK_SIZE)
    val waitDuration = FiniteDuration(userConf.getLong("snackfs.waitInterval", AT_MOST), MILLISECONDS)
    val queryDuration = FiniteDuration(userConf.getLong("snackfs.queryWaitInterval", QUERY_WAIT_TIMEOUT), MILLISECONDS)
    val useSSTable = userConf.get("snackfs.useSSTable", "false").toBoolean
    if(useSSTable) {
      val cassandraConf = userConf.get("cassandra.config")
      if(cassandraConf == null) {
        throw new Exception("SSTable enabled: cassandra.config is undefined")
      } else {
        System.setProperty("cassandra.config", cassandraConf)
        System.setProperty("cassandra.storagedir", sstableLocation)
      }
    }
    val uncompressedFileFormats: Set[String] = {
      toString(userConf.get("snackfs.fileformats.uncompressed")) match {
        case Some(value) => StringUtils.split(value, ",").toSet[String]
        case None => Collections.emptySet[String]
      }
    }
    val useACL = userConf.get("snackfs.acls.enabled", "true").toBoolean
    val clientMode = userConf.get("snackfs.client.mode", "local")
    val snackFSServerPort = userConf.getInt("snackfs.server.port", SNACKFS_SERVER_PORT)
    val snackFSClientTimeout = userConf.getInt("snackfs.server.timeout", 100)
    val cassandraHosts: List[String] = {
      toString(userConf.get("snackfs.cassandra.hosts")) match {
        case Some(value) => {
          val hosts = new ArrayList[String]
        	StringUtils.split(value, ",").foreach { host => hosts.add(InetAddress.getByName(host).getHostAddress) }
          hosts
        }
        case None => Collections.emptyList[String]
      }
    }
    var localhost = false
    cassandraHosts.foreach { host => 
      if(!localhost && LOCAL_HOST.equals(host)) {
        localhost = true
      }
    }
    val host = if(localhost || cassandraHosts.size == 0) LOCAL_HOST else cassandraHosts.get(Random.nextInt(cassandraHosts.size))
    SnackFSConfiguration(host, thriftPort, cqlPort, readLevel, writeLevel, keyspace, blockSize, subBlockSize, waitDuration, queryDuration, replicationFactor, replicationDataCenter, replicationStrategy, useSSTable && localhost, sstableLocation, partitioner, useLocking, uncompressedFileFormats, useACL, clientMode, snackFSServerPort, snackFSClientTimeout)
  }

  private def getConsistencyLevel(level: String): ConsistencyLevel = {
    if (level != null) {
      ConsistencyLevel.valueOf(level)
    } else {
      CONSISTENCY_LEVEL
    }
  }
  
  private def toString(in: String): Option[String] = {
    if(!StringUtils.isEmpty(in)) {
      Some(in)
    } else {
      None
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
