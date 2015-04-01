package com.tuplejump.snackfs.cassandra.pool

import scala.collection.JavaConversions.asScalaBuffer
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.concurrent.Promise

import org.apache.cassandra.db.marshal.UUIDType
import org.apache.cassandra.dht.LongToken
import org.apache.cassandra.service.StorageService
import org.apache.cassandra.thrift.Cassandra.AsyncClient
import org.apache.cassandra.thrift.Cassandra.AsyncClient.describe_ring_call
import org.apache.commons.pool.ObjectPool
import org.apache.commons.pool.impl.StackObjectPool
import org.apache.hadoop.fs.Path

import com.tuplejump.snackfs.cassandra.model.ClientPoolFactory
import com.tuplejump.snackfs.cassandra.model.SnackFSConfiguration
import com.tuplejump.snackfs.cassandra.model.ThriftClientAndSocket
import com.tuplejump.snackfs.fs.model.BlockMeta
import com.tuplejump.snackfs.fs.model.INode
import com.tuplejump.snackfs.util.AsyncUtil
import com.tuplejump.snackfs.util.LogConfiguration
import com.twitter.logging.Logger

class ThriftConnectionPool(configuration: SnackFSConfiguration) {

  private lazy val log = Logger.get(getClass)
  
  private val partitioner = StorageService.getPartitioner
  
  private var ring: scala.collection.mutable.Buffer[(java.util.List[String], Long, Long)] = null
  
  private val clientPool: ObjectPool[ThriftClientAndSocket] = new StackObjectPool[ThriftClientAndSocket](new ClientPoolFactory(configuration.CassandraHost, configuration.CassandraThriftPort, configuration.keySpace)) {
    override def close() {
      super.close()
      getFactory.asInstanceOf[ClientPoolFactory].closePool()
    }
  }
  
  def borrowObject(): ThriftClientAndSocket = {
    clientPool.borrowObject
  }
  
  def returnObject(connection: ThriftClientAndSocket) = {
    clientPool.returnObject(connection)
  }
  
  def close() = {
     clientPool.close 
  }
  
  def executeWithClient[T](f: AsyncClient => Future[T])(implicit tm: ClassManifest[T]): Future[T] = {
    val thriftClientAndSocket = clientPool.borrowObject()
    val ret = f(thriftClientAndSocket.client)

    ret.onComplete {
      res =>
        clientPool.returnObject(thriftClientAndSocket)
    }
    ret
  }
  
  def getBlockLocations(path: Path, inodeFuture: Future[INode]): Future[Map[BlockMeta, List[String]]] = executeWithClient({
    client =>
      val result = Promise[Map[BlockMeta, List[String]]]()

      inodeFuture.onSuccess {
        case inode =>
          if(ring == null) {
            if(LogConfiguration.isDebugEnabled) log.debug(Thread.currentThread.getName() + " found iNode for %s, getting block locations", path)
            val ringFuture = AsyncUtil.executeAsync[describe_ring_call](
              client.describe_ring(configuration.keySpace, _)
            )

            ringFuture.onSuccess {
              case r =>
                if(LogConfiguration.isDebugEnabled) log.debug(Thread.currentThread.getName() + " fetched ring details for keyspace %s", configuration.keySpace)
                ring = r.getResult.map(tokenRange => (tokenRange.getEndpoints, tokenRange.getStart_token.toLong, tokenRange.getEnd_token.toLong))
                result success findBlocks(path, inode)
            }
  
            ringFuture.onFailure {
              case f =>
                log.error(f, "failed to get ring details for keyspace %s", configuration.keySpace)
                result failure f
            }
          } else {
            result success findBlocks(path, inode)
          }
      }

      inodeFuture.onFailure {
        case e =>
          log.error(e, "iNode for %s not found", path)
          result failure e
      }

      result.future
  })
  
  def findBlocks(path: Path, inode: INode): Map[BlockMeta, List[String]] = {
    var response = Map.empty[BlockMeta, List[String]]

    //For each block in the file, get the owner node
    inode.blocks.foreach(b => {
      val decoratedKey = partitioner.decorateKey(UUIDType.instance.decompose(b.id))
      val token = decoratedKey.getToken.asInstanceOf[LongToken]

      val xr = ring.filter {
        p =>
          if (p._2 < p._3) {
            val eval = p._2 <= token.token && p._3 >= token.token
            if(eval && LogConfiguration.isDebugEnabled) log.debug(Thread.currentThread.getName() + "host: %s -> %s <= %s and %s >= %s", p._1, p._2, token.token, p._3, token.token )
            eval
          } else {
            val eval = (p._2 <= token.token && Long.MaxValue >= token.token) || (p._3 >= token.token && Long.MinValue <= token.token)
            if(eval && LogConfiguration.isDebugEnabled) log.debug(Thread.currentThread.getName() + "host: %s -> %s <= %s and %s >= %s || %s >= %s and %s <= %s",  p._1, p._2, token.token, Long.MaxValue, token.token, p._3, token.token, Long.MinValue, token.token )
            eval
          }
      }

      var endpoints: List[String] = List.empty
      if (xr.length > 0) {
        endpoints = xr.flatMap(_._1).toList
      } else {
        endpoints = ring(0)._1.toList
      }
      response += (b -> endpoints)
      if(LogConfiguration.isDebugEnabled) log.debug(Thread.currentThread.getName() + " for iNode: %s found block: %s token: %s at host: %s", path, b.id, token.token, endpoints)
    })
    response
  }

}