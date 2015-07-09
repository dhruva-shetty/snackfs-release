package com.tuplejump.snackfs.cassandra.store

import java.net.InetAddress

import com.datastax.driver.core.Cluster
import com.datastax.driver.core.ConsistencyLevel
import com.datastax.driver.core.HostDistance
import com.datastax.driver.core.PreparedStatement
import com.datastax.driver.core.Session
import com.datastax.driver.core.policies.DefaultRetryPolicy
import com.tuplejump.snackfs.cassandra.loadbalancing.LocalMachineRoundRobinPolicy
import com.tuplejump.snackfs.cassandra.model.SnackFSConfiguration
import com.tuplejump.snackfs.cassandra.pool.ThriftConnectionPool
import com.tuplejump.snackfs.cassandra.store.CassandraStoreMetadata._

object CassandraCluster {
  
  private var cluster: Cluster = null
  
  private var session: Session = null
  
  private var clientPool: ThriftConnectionPool = null
  
  var insertInodePrepared: PreparedStatement = null
    
  var insertSblockPrepared: PreparedStatement  = null
  
  var insertLockPrepared: PreparedStatement  = null
  
  var queryInodePrepared: PreparedStatement  = null
  
  var querySubBlockPrepared: PreparedStatement  = null
  
  def getThriftConnectionPool(configuration: SnackFSConfiguration) = {
    var newClientPool = clientPool
    if(newClientPool == null) {
      this.synchronized({
        newClientPool = clientPool
        if(newClientPool == null) {
          newClientPool = new ThriftConnectionPool(configuration)
          clientPool = newClientPool
        }
      })
    }
    newClientPool
  }
  
  def getCluster(configuration: SnackFSConfiguration):Cluster = {
    var newCluster:Cluster = cluster 
    if(newCluster == null) {
      this.synchronized( {
        newCluster = cluster
        if(newCluster == null) {
          newCluster = Cluster.builder.withoutMetrics.withoutJMXReporting
                                      .withLoadBalancingPolicy(new LocalMachineRoundRobinPolicy(InetAddress.getByName(configuration.CassandraHost)))
                                      .addContactPoint(configuration.CassandraHost)
                                      .withPort(configuration.CassandraCqlPort)
                                      .withRetryPolicy(DefaultRetryPolicy.INSTANCE)
                                      .build
          newCluster.getConfiguration.getSocketOptions.setReadTimeoutMillis(60000)
          newCluster.getConfiguration.getSocketOptions.setTcpNoDelay(true)
          newCluster.getConfiguration.getQueryOptions.setConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM)
          newCluster.getConfiguration.getPoolingOptions.setMaxConnectionsPerHost(HostDistance.LOCAL, 16)
          newCluster.getConfiguration.getPoolingOptions.setCoreConnectionsPerHost(HostDistance.LOCAL, 4)
          newCluster.getConfiguration.getPoolingOptions.setMaxConnectionsPerHost(HostDistance.REMOTE, 16)
          newCluster.getConfiguration.getPoolingOptions.setCoreConnectionsPerHost(HostDistance.REMOTE, 4)
          cluster = newCluster
        }
      })
    }
    newCluster
  }
  
  def getSession(configuration: SnackFSConfiguration):Session = {
    var newSession:Session = session 
    if(newSession == null) {
      this.synchronized( {
        newSession = session
        if(newSession == null) {
          newSession = getCluster(configuration).connect(configuration.keySpace)
          session = newSession
          initializePreparedStatements(newSession)
        }
      })
    }
    newSession
  }
  
  private def initializePreparedStatements(session: Session) = {
    insertInodePrepared   = session.prepare(s"INSERT INTO $INODE_TABLE($ID_COLUMN, $DATA_COLUMN, $PARENT_PATH_COLUMN, $PATH_COLUMN, $SENTINEL_COLUMN, $MODIFIED_COLUMN) VALUES (?, ?, ?, ?, ?, ?);")
    
    insertSblockPrepared  = session.prepare(s"INSERT INTO $SBLOCK_TABLE($BLOCK_ID_COLUMN, $SBLOCK_ID_COLUMN, $DATA_COLUMN) VALUES (?, ?, ?);")
    
    insertLockPrepared    = session.prepare(s"INSERT INTO $LOCK_TABLE($LOCK_ID_COLUMN, $PROCESS_ID_COLUMN) VALUES (?, ?);")
    
    queryInodePrepared    = session.prepare(s"""SELECT $DATA_COLUMN, $MODIFIED_COLUMN 
                                                FROM $INODE_TABLE 
                                                WHERE $ID_COLUMN = ? 
                                                ALLOW FILTERING;""")
                                                
    querySubBlockPrepared = session.prepare(s"""SELECT $DATA_COLUMN 
                                                FROM $SBLOCK_TABLE 
                                                WHERE $BLOCK_ID_COLUMN = ? 
                                                      AND $SBLOCK_ID_COLUMN = ? 
                                                ALLOW FILTERING;""")
  }
}