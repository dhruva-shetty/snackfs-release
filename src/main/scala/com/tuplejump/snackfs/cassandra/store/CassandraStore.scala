package com.tuplejump.snackfs.cassandra.store

import java.io.InputStream
import java.math.BigInteger
import java.net.InetAddress
import java.nio.ByteBuffer
import java.util.UUID

import scala.collection.JavaConversions.asScalaBuffer
import scala.compat.Platform
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.concurrent.Promise

import org.apache.cassandra.thrift.NotFoundException
import org.apache.cassandra.utils.ByteBufferUtil
import org.apache.cassandra.utils.FBUtilities
import org.apache.hadoop.fs.Path

import com.datastax.driver.core.BoundStatement
import com.datastax.driver.core.Cluster
import com.datastax.driver.core.ConsistencyLevel
import com.datastax.driver.core.HostDistance
import com.datastax.driver.core.PreparedStatement
import com.datastax.driver.core.ResultSet
import com.datastax.driver.core.Row
import com.datastax.driver.core.Session
import com.datastax.driver.core.Statement
import com.datastax.driver.core.policies.DefaultRetryPolicy
import com.datastax.driver.core.querybuilder.QueryBuilder
import com.datastax.driver.core.querybuilder.Select
import com.tuplejump.snackfs.cassandra.compression.LZ4CompressorInstance
import com.tuplejump.snackfs.cassandra.loadbalancing.LocalMachineRoundRobinPolicy
import com.tuplejump.snackfs.cassandra.model.GenericOpSuccess
import com.tuplejump.snackfs.cassandra.model.Keyspace
import com.tuplejump.snackfs.cassandra.model.SnackFSConfiguration
import com.tuplejump.snackfs.cassandra.partial.FileSystemStore
import com.tuplejump.snackfs.cassandra.pool.ThriftConnectionPool
import com.tuplejump.snackfs.cassandra.sstable.DirectSSTableReader
import com.tuplejump.snackfs.fs.model.BlockMeta
import com.tuplejump.snackfs.fs.model.INode
import com.tuplejump.snackfs.fs.model.SubBlockMeta
import com.tuplejump.snackfs.fs.stream.BlockInputStream
import com.tuplejump.snackfs.security.FSPermissionChecker
import com.tuplejump.snackfs.server.SnackFSClient
import com.tuplejump.snackfs.util.AsyncUtil
import com.tuplejump.snackfs.util.LogConfiguration
import com.twitter.logging.Logger

class CassandraStore(configuration: SnackFSConfiguration) extends FileSystemStore {
  
  LogConfiguration.config()
  
  private lazy val log = Logger.get(getClass)
  
  private val KEYSPACE            = configuration.keySpace
  private val INODE_TABLE         = "inode"
  private val SBLOCK_TABLE        = "sblock"
  private val LOCK_TABLE          = "lock"

  private val ID_COLUMN           = "id"
  private val PATH_COLUMN         = "path"
  private val PARENT_PATH_COLUMN  = "parent_path"
  private val SENTINEL_COLUMN     = "sentinel"
  private val DATA_COLUMN         = "data"
  private val MODIFIED_COLUMN     = "modified"
  private val BLOCK_ID_COLUMN     = "block_id"
  private val SBLOCK_ID_COLUMN    = "sblock_id"
  private val SENTINEL_VALUE      = "x"
  private val LOCK_ID_COLUMN      = "lock_id"
  private val PROCESS_ID_COLUMN   = "process_id"
  
  private val ks_cql          = s"CREATE KEYSPACE IF NOT EXISTS ${KEYSPACE} WITH replication = {'class':'NetworkTopologyStrategy', 'NDH':'1', 'EWD':'1'};"
  private val drop_ks_cql     = s"DROP KEYSPACE ${KEYSPACE}"
  private val parent_path_idx = "CREATE INDEX IF NOT EXISTS parent_path  ON " + KEYSPACE + "." + INODE_TABLE + " (" + PARENT_PATH_COLUMN + ");";
  private val sentinel_idx    = "CREATE INDEX IF NOT EXISTS sentinel  ON " + KEYSPACE + "." + INODE_TABLE + " (" + SENTINEL_COLUMN + ");";
  private val inode_cql       = "CREATE TABLE IF NOT EXISTS " + KEYSPACE + "." + INODE_TABLE + "(" + ID_COLUMN + " varint, " + PATH_COLUMN + " text, " + PARENT_PATH_COLUMN + " text, " + SENTINEL_COLUMN + " text, " + MODIFIED_COLUMN + " bigint, "+ DATA_COLUMN + " blob, " + " PRIMARY KEY (" + ID_COLUMN + "," + PATH_COLUMN + ") " + " ) " + " WITH compression = { 'sstable_compression' : '' } AND comment='Stores file meta data';"
  private val sblock_cql      = "CREATE TABLE IF NOT EXISTS " + KEYSPACE + "." + SBLOCK_TABLE + "(" + BLOCK_ID_COLUMN + " timeuuid, " + SBLOCK_ID_COLUMN + " timeuuid, " + DATA_COLUMN + " blob, " + " PRIMARY KEY (" + BLOCK_ID_COLUMN + "," + SBLOCK_ID_COLUMN + ") " + " ) " + " WITH compression = { 'sstable_compression' : '' } AND comment='Stores blocks of information associated with a inode'" + " AND CLUSTERING ORDER BY (" + SBLOCK_ID_COLUMN + " ASC);"
  private val lock_cql        = "CREATE TABLE IF NOT EXISTS " + KEYSPACE + "." + LOCK_TABLE + "(" + LOCK_ID_COLUMN + " varint, " + PROCESS_ID_COLUMN + " timeuuid, " + " PRIMARY KEY (" + LOCK_ID_COLUMN + "," + PROCESS_ID_COLUMN +  ") " + " ) " + " WITH compression = { 'sstable_compression' : '' } AND comment='Stores file locking metadata';"

  private var cluster: Cluster = Cluster.builder.withoutMetrics.withoutJMXReporting
                                        .withLoadBalancingPolicy(new LocalMachineRoundRobinPolicy(InetAddress.getByName(configuration.CassandraHost)))
                                        .addContactPoint(configuration.CassandraHost)
                                        .withPort(configuration.CassandraCqlPort)
                                        .withRetryPolicy(DefaultRetryPolicy.INSTANCE)
                                        .build
                                        
  private val usePreparedStatement = true
  private val writeConsistencyLevel = getConsistency(configuration.writeConsistencyLevel);
  private val readConsistencyLevel = getConsistency(configuration.readConsistencyLevel);
  private var clientPool: ThriftConnectionPool = new ThriftConnectionPool(configuration)
  
  private var session: Session = null
  private var insertInodePrepared: PreparedStatement = null
  private var insertSblockPrepared: PreparedStatement  = null
  private var insertLockPrepared: PreparedStatement  = null
  private var queryInodePrepared: PreparedStatement  = null
  private var querySubBlockPrepared: PreparedStatement  = null
  
  private var fsPermissionChecker: FSPermissionChecker = null
  private var internalClient: SnackFSClient = null
  private var sstableReader: DirectSSTableReader = null
  
  def getConsistency(consistencyLevel: org.apache.cassandra.thrift.ConsistencyLevel): ConsistencyLevel = {
    consistencyLevel match {
      case org.apache.cassandra.thrift.ConsistencyLevel.ONE           =>  ConsistencyLevel.ONE
      case org.apache.cassandra.thrift.ConsistencyLevel.QUORUM        =>  ConsistencyLevel.QUORUM
      case org.apache.cassandra.thrift.ConsistencyLevel.LOCAL_QUORUM  =>  ConsistencyLevel.LOCAL_QUORUM
      case org.apache.cassandra.thrift.ConsistencyLevel.EACH_QUORUM   =>  ConsistencyLevel.EACH_QUORUM
      case org.apache.cassandra.thrift.ConsistencyLevel.ALL           =>  ConsistencyLevel.ALL
      case org.apache.cassandra.thrift.ConsistencyLevel.ANY           =>  ConsistencyLevel.ANY
      case org.apache.cassandra.thrift.ConsistencyLevel.TWO           =>  ConsistencyLevel.TWO
      case org.apache.cassandra.thrift.ConsistencyLevel.THREE         =>  ConsistencyLevel.THREE
      case org.apache.cassandra.thrift.ConsistencyLevel.SERIAL        =>  ConsistencyLevel.SERIAL
      case org.apache.cassandra.thrift.ConsistencyLevel.LOCAL_SERIAL  =>  ConsistencyLevel.LOCAL_SERIAL
      case org.apache.cassandra.thrift.ConsistencyLevel.LOCAL_ONE     =>  ConsistencyLevel.LOCAL_ONE
    }
  } 
  
  def init {
    cluster.getConfiguration.getSocketOptions.setReadTimeoutMillis(60000)
    cluster.getConfiguration.getSocketOptions.setTcpNoDelay(true)
    cluster.getConfiguration.getQueryOptions.setConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM)
    cluster.getConfiguration.getPoolingOptions.setMaxConnectionsPerHost(HostDistance.LOCAL, 16)
    cluster.getConfiguration.getPoolingOptions.setCoreConnectionsPerHost(HostDistance.LOCAL, 4)
    cluster.getConfiguration.getPoolingOptions.setMaxConnectionsPerHost(HostDistance.REMOTE, 16)
    cluster.getConfiguration.getPoolingOptions.setCoreConnectionsPerHost(HostDistance.REMOTE, 4)
    
    val user = System.getProperty("user.name")
    if (LogConfiguration.isDebugEnabled) log.debug("Cassandra config -> keyspace: %s, thrift-port: %s, cql-port: %s, useSSTable: %s, sstableLocation: %s, useLocking: %s", configuration.keySpace, configuration.CassandraThriftPort, configuration.CassandraCqlPort, configuration.useSSTable, configuration.sstableLocation, configuration.useLocking)
    if (LogConfiguration.isDebugEnabled) log.debug("SnackFS config -> host: %s, user: %s, subBlockSize: %s, blockSize: %s, uncompressed-file-formats: %s", configuration.CassandraHost, user, configuration.subBlockSize, configuration.blockSize, configuration.uncompressedFileFormats)
    
    fsPermissionChecker = new FSPermissionChecker(this, user, configuration.useACL)
    
    if(config.useSSTable) {
      if(config.standalone) {
        sstableReader = DirectSSTableReader(false, configuration.keySpace, configuration.sstableLocation)
      } else {
        internalClient = new SnackFSClient(configuration)
      }
    }
  }
  
  def initializePreparedStatements(session: Session) = {
	  insertInodePrepared = session.prepare("INSERT INTO inode(id, data, parent_path, path, sentinel, modified) VALUES (?, ?, ?, ?, ?, ?);")
    insertSblockPrepared = session.prepare("INSERT INTO sblock(block_id, sblock_id, data) VALUES (?, ?, ?);")
    insertLockPrepared = session.prepare("INSERT INTO lock(lock_id, process_id) VALUES (?, ?);")
    queryInodePrepared = session.prepare("SELECT " + DATA_COLUMN + ", " + MODIFIED_COLUMN + " FROM " + INODE_TABLE + " WHERE " + ID_COLUMN + "=? ALLOW FILTERING;")
    querySubBlockPrepared = session.prepare("SELECT " + DATA_COLUMN + " FROM " + SBLOCK_TABLE + " WHERE  "+ BLOCK_ID_COLUMN + "=? " + " AND " + SBLOCK_ID_COLUMN + "=? ALLOW FILTERING;")
  }
  
  def getSession: Session = {
    if(session == null) {
      session = cluster.connect(KEYSPACE)
    } 
    session
  }
  
  def disconnect() = cluster.close()
  
  def createKeyspace: Future[Keyspace] = {
    val prom = Promise[Keyspace]()
    val f = Future  {
          val s = cluster.connect
          s.execute(ks_cql)
          s.execute(inode_cql)
          s.execute(parent_path_idx)
          s.execute(sentinel_idx)
          s.execute(sblock_cql)
          s.execute(lock_cql)
          s.close
    } (AsyncUtil.getExecutionContext)
    f onSuccess {
      case p =>
          session = cluster.connect(KEYSPACE)
          if(usePreparedStatement) initializePreparedStatements(session)
          prom success new Keyspace(configuration.keySpace)
    }
    f onFailure {
      case f =>
          log.error("Keyspace CQL: %s", ks_cql)
          log.error("Inode CQL: %s", inode_cql)
          log.error("ParentPath-Index CQL: %s", parent_path_idx)
          log.error("Sentinel-Index CQL: %s", sentinel_idx)
          log.error("Sblock CQL: %s", sblock_cql)
          log.error(f, "Failed to create Keyspace %s", configuration.keySpace)
          prom failure f
    }
    prom.future
  }

  private def getParentForIndex(path: Path): String = {
    val parent = path.getParent
    var result = "null"
    if (parent != null) {
      result = parent.toUri.getPath
    }
    result
  }
  
  private def getPathKey(path: Path): BigInteger = {
    FBUtilities.hashToBigInteger(ByteBufferUtil.bytes(path.toUri.getPath))
  }
  
  def storeINode(path: Path, iNode: INode): Future[GenericOpSuccess] = {
    val timestamp = iNode.timestamp
    var insertInode: Statement = null
    val dataBuffer = iNode.serialize
    val bytes:Array[Byte] = Array.ofDim[Byte](dataBuffer.remaining)
    dataBuffer.get(bytes)
    val compressedData = ByteBuffer.wrap(LZ4CompressorInstance.compress(bytes, true))
    if(usePreparedStatement) {
      insertInode = new BoundStatement(insertInodePrepared).bind(getPathKey(path),
                                                                 compressedData,
                                                                 getParentForIndex(path),
                                                                 path.toUri.getPath,
                                                                 SENTINEL_VALUE,
                                                                 timestamp.asInstanceOf[Object])
    } else {
     insertInode = QueryBuilder.insertInto(INODE_TABLE)
                               .value(ID_COLUMN,           getPathKey(path))
                               .value(DATA_COLUMN,         compressedData)
                               .value(PARENT_PATH_COLUMN,  getParentForIndex(path))
                               .value(PATH_COLUMN,         path.toUri.getPath)
                               .value(SENTINEL_COLUMN,     SENTINEL_VALUE)
                               .value(MODIFIED_COLUMN,     timestamp)
    }
    val insertInodeFuture =  getSession.executeAsync(insertInode.setConsistencyLevel(writeConsistencyLevel));
    val result  = Promise[GenericOpSuccess]()
    insertInodeFuture.addListener(new Runnable() {
      override def run() = {
        try {
          val p = insertInodeFuture.getUninterruptibly
          if(p.wasApplied) { 
            if(LogConfiguration.isDebugEnabled) log.debug(Thread.currentThread.getName() + " stored INode %s", iNode.toString)
            result success GenericOpSuccess()
          } else {
            result failure new Exception("Failed to store inode " + iNode.toString)
          }
        } catch {
          case f: Exception =>
            log.error(f, "Failed to store INode %s", iNode.toString)
            result failure f
        }
      }
    }, AsyncUtil.getExecutionContext)
    result.future
  }

  def deleteINode(path: Path): Future[GenericOpSuccess] = {
    val deleteInode = QueryBuilder.delete().from(INODE_TABLE)
                                           .where(QueryBuilder.eq(ID_COLUMN, getPathKey(path)))
                                           
    val deleteInodeFuture =  getSession.executeAsync(deleteInode.setConsistencyLevel(writeConsistencyLevel));
    if(LogConfiguration.isDebugEnabled) log.debug(Thread.currentThread.getName() + " executing query: %s", deleteInode)
    
    val result  = Promise[GenericOpSuccess]()
    deleteInodeFuture.addListener(new Runnable() {
      override def run() = {
        try {
          val p = deleteInodeFuture.getUninterruptibly
          if(LogConfiguration.isDebugEnabled) log.debug(Thread.currentThread.getName() + " deleted INode with path %s", path)
          result success GenericOpSuccess()
        } catch {
          case f: Exception =>
           log.error(f, "failed to delete INode with path %s", path)
           result failure f
        }
      }
    }, AsyncUtil.getExecutionContext)
   
    result.future
  }
  
  def retrieveINode(path: Path): Future[INode] = {
    var retrieveInode:Statement = null
    if(usePreparedStatement) {
      retrieveInode = new BoundStatement(queryInodePrepared).bind(getPathKey(path))
    } else {
      retrieveInode = QueryBuilder.select().column(DATA_COLUMN).column(MODIFIED_COLUMN).from(INODE_TABLE).allowFiltering().where(QueryBuilder.eq(ID_COLUMN, getPathKey(path)))
		  if(LogConfiguration.isDebugEnabled) log.debug(Thread.currentThread.getName() + " executing query: %s", retrieveInode)
    }
    val pathInfo =  getSession.executeAsync(retrieveInode.setConsistencyLevel(readConsistencyLevel))
    val start = Platform.currentTime
    val result = Promise[INode]()
    pathInfo.addListener(new Runnable() {
      override def run() = {
        try {
          val row = pathInfo.getUninterruptibly.one
          if(row != null) {
            if(LogConfiguration.isDebugEnabled) log.debug(Thread.currentThread.getName() + " Elapsed time to retrieve Inode %s: %s ms", path, (Platform.currentTime - start))
            val dataBuffer = row.getBytes(DATA_COLUMN)
            val bytes:Array[Byte] = Array.ofDim[Byte](dataBuffer.remaining)
            dataBuffer.get(bytes)
            result success INode.deserialize(ByteBufferUtil.inputStream(ByteBuffer.wrap(LZ4CompressorInstance.decompress(bytes, true))), row.getLong(MODIFIED_COLUMN))
          } else {
            throw new Exception("Failed to get INode/subblock data for " + path)
          }
        } catch {
          case f: Exception =>
            log.error(f, "failed to retrieve Inode for path %s", path)
            result failure f
        }
      }
    }, AsyncUtil.getExecutionContext)
    
    result.future
  }
  
  def retrieveBlock(blockMeta: BlockMeta, compressed: Boolean, isLocalBlock: Boolean): InputStream = {
    if(LogConfiguration.isDebugEnabled) log.debug(Thread.currentThread.getName() + " retrieve Block %s useSSTable %s isLocalBlock %s", blockMeta.toString, configuration.useSSTable, isLocalBlock)
    BlockInputStream(this, blockMeta, compressed, configuration.useSSTable && isLocalBlock, configuration.atMost, configuration.blockSize, configuration.subBlockSize)
  }
  
  def storeSubBlock(blockId: UUID, subBlockMeta: SubBlockMeta, dataBuffer: ByteBuffer, compressed: Boolean): Future[GenericOpSuccess] = {
    var insertSubBlock: Statement = null
    var start = Platform.currentTime
    val bytes:Array[Byte] = Array.ofDim[Byte](dataBuffer.remaining)
    dataBuffer.get(bytes)
    val compressedData = ByteBuffer.wrap(LZ4CompressorInstance.compress(bytes, compressed))
    if(LogConfiguration.isDebugEnabled) log.debug(Thread.currentThread.getName() + " Elapsed time to compress: %s ms", (Platform.currentTime - start))
    if(usePreparedStatement) {
      insertSubBlock = new BoundStatement(insertSblockPrepared).bind(blockId, subBlockMeta.id, compressedData)
    } else {
      insertSubBlock = QueryBuilder.insertInto(SBLOCK_TABLE)
                                   .value(BLOCK_ID_COLUMN, blockId)
                                   .value(SBLOCK_ID_COLUMN, subBlockMeta.id)
                                   .value(DATA_COLUMN, compressedData)
    }
    val prom = Promise[GenericOpSuccess]()
    val subBlockFuture = getSession.executeAsync(insertSubBlock.setConsistencyLevel(writeConsistencyLevel))
    subBlockFuture.addListener(new Runnable() {
      override def run() = {
        try {
          val p = subBlockFuture.getUninterruptibly
          if(LogConfiguration.isDebugEnabled) log.debug(Thread.currentThread.getName() + " stored subBlock %s for block with id %s", subBlockMeta.toString, blockId.toString)
          prom success GenericOpSuccess()
        } catch {
          case f: Exception =>
            if(LogConfiguration.isDebugEnabled) log.debug(f, " failed to store subBlock %s for block with id %s", subBlockMeta.toString, blockId.toString)
            prom failure f
        }
      }
    }, AsyncUtil.getExecutionContext)
    
    prom.future
  }  
  
  def retrieveSubBlock(blockId: UUID, subBlockId: UUID, compressed: Boolean): Future[InputStream] = {
    if(LogConfiguration.isDebugEnabled) log.debug(Thread.currentThread.getName() + " fetching subBlock for path %s", subBlockId.toString)
    var retrieveSubBlock: Statement = null
    if(usePreparedStatement) {
      retrieveSubBlock = new BoundStatement(querySubBlockPrepared).bind(blockId, subBlockId)
    } else {
      retrieveSubBlock = QueryBuilder.select().column(DATA_COLUMN).from(SBLOCK_TABLE)
                                                                  .allowFiltering()
                                                                  .where(QueryBuilder.eq(BLOCK_ID_COLUMN, blockId))
                                                                  .and(QueryBuilder.eq(SBLOCK_ID_COLUMN, subBlockId))
      if(LogConfiguration.isDebugEnabled) log.debug(Thread.currentThread.getName() + " executing query: %s", retrieveSubBlock)
    }
    val start = Platform.currentTime
    val subBlockFuture = getSession.executeAsync(retrieveSubBlock.setConsistencyLevel(readConsistencyLevel))
    val prom = Promise[InputStream]()
    subBlockFuture.addListener(new Runnable() {
      override def run() = {
        try {
          var row = subBlockFuture.getUninterruptibly.one
          if(row == null) {
        	  log.error(Thread.currentThread.getName() + " Received empty data while retrieving blockId %s, sblockId %s retrying", blockId, subBlockId)
            row = AsyncUtil.retry(3)(retrieveResult(getSession.execute(retrieveSubBlock.setConsistencyLevel(readConsistencyLevel))))
          }
          val dataBuffer = row.getBytes(DATA_COLUMN)
          val bytes:Array[Byte] = Array.ofDim[Byte](dataBuffer.remaining)
          dataBuffer.get(bytes)
          val decompressed = LZ4CompressorInstance.decompress(bytes, compressed)
          val stream: InputStream = ByteBufferUtil.inputStream(ByteBuffer.wrap(decompressed))
          prom success stream
        } catch {
          case f: Exception =>
           if(LogConfiguration.isDebugEnabled) log.debug(Thread.currentThread.getName() + " failed to retrieve blockId %s, sblockId %s", blockId, subBlockId)
           prom failure new NotFoundException
        }
      }
    }, AsyncUtil.getExecutionContext)

    prom.future
  }
  
  def retrieveResult(result: ResultSet): Row = {
    var row = result.one
    if(row == null) {
      throw new Exception("Failed to retrieve data column")
    }
    row
  }

  def deleteBlocks(iNode: INode): Future[GenericOpSuccess] = {
    var batchDelete = {
       var batch = QueryBuilder.batch()
       iNode.blocks.foreach { block => batch.add(QueryBuilder.delete.from(SBLOCK_TABLE).where(QueryBuilder.eq(BLOCK_ID_COLUMN, block.id))) }
       batch
    }
    val batchDeleteFuture = getSession.executeAsync(batchDelete.setConsistencyLevel(writeConsistencyLevel))
    val result = Promise[GenericOpSuccess]()
    
    batchDeleteFuture.addListener(new Runnable() {
      override def run() = {
        try {
          val p = batchDeleteFuture.getUninterruptibly.all
          result success GenericOpSuccess()
        } catch {
          case f: Exception =>
            log.error(f, "failed to delete blocks for INode %s", iNode.toString)
            result failure f
        }
      }
    }, AsyncUtil.getExecutionContext)
    
    result.future
  }
  
  private def fetchPaths(path: Path, query: Statement): Future[Set[Path]] = {
    val queryFuture = getSession.executeAsync(query.setConsistencyLevel(readConsistencyLevel))
    val result = Promise[Set[Path]]()
    val f = Future ({
      if(LogConfiguration.isDebugEnabled) log.debug(Thread.currentThread.getName() + " executing query: %s", query)
      queryFuture.getUninterruptibly
    }) (AsyncUtil.getExecutionContext)
    
    queryFuture.addListener(new Runnable() {
      override def run() = {
        try {
          val p = queryFuture.getUninterruptibly
          var paths: Seq[Path] = Nil
          val rows = p.all
          rows.foreach { row => 
            val col = row.getString(PATH_COLUMN)
            paths = paths :+ new Path(col) 
          }
          if(LogConfiguration.isDebugEnabled) log.debug(Thread.currentThread.getName() + " finished fetching path %s, total rows %s, total columns added %s", path, rows.length, paths.length)
          result success paths.toSet
        } catch {
          case f: Exception =>
            log.error(f, "failed to fetch subpaths for  %s", path)
            result failure f
        }
      }
    }, AsyncUtil.getExecutionContext)
    
    result.future
  }
  
  def fetchSubPaths(path: Path, isDeepFetch: Boolean): Future[Set[Path]] = {
    val startPath = path.toUri.getPath
    var query: Select.Where = QueryBuilder.select().column(PATH_COLUMN).from(INODE_TABLE)
                                                                       .allowFiltering()
                                                                       .where(QueryBuilder.eq(SENTINEL_COLUMN, SENTINEL_VALUE))
    if (isDeepFetch) {
    	query = query.and(QueryBuilder.gt(PATH_COLUMN, startPath));
      if (startPath.length > 1) {
        val lastChar = (startPath(startPath.length - 1) + 1).asInstanceOf[Char]
        val endPath = startPath.substring(0, startPath.length - 1) + lastChar
        query = query.and(QueryBuilder.lt(PATH_COLUMN, endPath))
      }
    } else {
      query = query.and(QueryBuilder.eq(PARENT_PATH_COLUMN, startPath))
    }
    
    def recursionStrategy: String = {
      if (isDeepFetch) {
        "recursively"
      } else {
        "non-recursively"
      }
    }
    if(LogConfiguration.isDebugEnabled) log.debug(Thread.currentThread.getName() + " fetching subPaths for %s, %s ", path, recursionStrategy)
    fetchPaths(path, query)
  }
  
  def getBlockLocations(path: Path): Future[Map[BlockMeta, List[String]]] = clientPool.getBlockLocations(path, retrieveINode(path))
  
  def acquireFileLock(path: Path, processId: UUID): Future[Boolean] = {
    var insertLock: Statement = null
    if(usePreparedStatement) {
      insertLock = new BoundStatement(insertLockPrepared).bind(getPathKey(path), processId)
    } else {
      insertLock = QueryBuilder.insertInto(LOCK_TABLE)
                               .value(LOCK_ID_COLUMN, getPathKey(path))
                               .value(PROCESS_ID_COLUMN, processId)
    }

    val prom = Promise[Boolean]()
    val lockFuture = getSession.executeAsync(insertLock.setConsistencyLevel(writeConsistencyLevel))
    lockFuture.addListener(new Runnable() {
      override def run() = {
        try {
          val p = lockFuture.getUninterruptibly
          val selectLock = QueryBuilder.select(PROCESS_ID_COLUMN).from(LOCK_TABLE).where(QueryBuilder.eq(LOCK_ID_COLUMN, getPathKey(path)))
          val resultSet = getSession.execute(selectLock.setConsistencyLevel(writeConsistencyLevel))
          var result = false
          if(resultSet != null) {
             val row = resultSet.iterator().next();
             if(row != null) {
               val entryId = row.getUUID(PROCESS_ID_COLUMN)
               if (entryId != null) {
                 val entryIdString: String = entryId.toString()
                 val processIdString: String = processId.toString()
                 if(LogConfiguration.isDebugEnabled) log.debug(Thread.currentThread.getName() + " processId with lock %s", entryIdString)
                 if(LogConfiguration.isDebugEnabled) log.debug(Thread.currentThread.getName() + " processId acquiring lock %s", processIdString)
                 if(entryIdString == processIdString) {
                    result = true
                 }
               }
             }
          }
          prom success result
        } catch {
          case f: Exception =>
            releaseFileLock(path);
            log.error(f, "error in adding column for create lock: " + path)
            prom failure f
        }
      }
    }, AsyncUtil.getExecutionContext)
    
    prom.future
  }

  def releaseFileLock(path: Path): Future[Boolean] = {
    val deleteLock = QueryBuilder.delete().from(LOCK_TABLE).where(QueryBuilder.eq(LOCK_ID_COLUMN, getPathKey(path)))
    val deleteLockFuture = getSession.executeAsync(deleteLock.setConsistencyLevel(writeConsistencyLevel))
    val prom = Promise[Boolean]()
    deleteLockFuture.addListener(new Runnable() {
      override def run() = {
        try {
          val p = deleteLockFuture.getUninterruptibly
          if(LogConfiguration.isDebugEnabled) log.debug(Thread.currentThread.getName() + " deleted lock: " + path)
          prom success true
        } catch {
          case f: Exception =>
            log.error(f, "failed to delete lock: " + path)
            prom success false
        }
      }
    }, AsyncUtil.getExecutionContext)
    prom.future
  }
  
  def config: SnackFSConfiguration = configuration
  
  def permissionChecker: FSPermissionChecker = fsPermissionChecker
  
  def dropKeyspace: Future[Unit] = {
    val prom = Promise[Unit]()
    val f = Future  {
          val s = cluster.connect
          s.execute(drop_ks_cql)
          s.close
    } (AsyncUtil.getExecutionContext)
    f onSuccess {
      case p =>
        prom success p
    }
    f onFailure {
      case f =>
        log.error("Failed to drop Keyspace: %s", drop_ks_cql)
        prom failure f
    }
    prom.future
  }
  
  def getRemoteActor: SnackFSClient = internalClient
  
  def getSSTableReader: DirectSSTableReader = sstableReader
}