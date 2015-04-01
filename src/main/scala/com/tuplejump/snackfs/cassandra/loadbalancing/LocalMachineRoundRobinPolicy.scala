package com.tuplejump.snackfs.cassandra.loadbalancing

import java.net.InetAddress

import scala.collection.JavaConversions.asJavaIterator
import scala.collection.JavaConversions.collectionAsScalaIterable

import com.datastax.driver.core.Cluster
import com.datastax.driver.core.Host
import com.datastax.driver.core.HostDistance
import com.datastax.driver.core.Statement
import com.datastax.driver.core.policies.RoundRobinPolicy

class LocalMachineRoundRobinPolicy(host: InetAddress) extends RoundRobinPolicy {

  private var liveNodes = Set.empty[Host]

  override def init(cluster: Cluster, hosts: java.util.Collection[Host]) {
    hosts.seq.foreach { h => 
      if(h.getAddress.equals(host)) {
        liveNodes += h
        return
      }
    }
  }

  override def distance(h: Host): HostDistance = {
    if(liveNodes.contains(h) || h.getAddress.equals(host)) {
      HostDistance.LOCAL
    } else {
      HostDistance.IGNORED
    }
  }

  override def newQueryPlan(query: String, statement: Statement): java.util.Iterator[Host] = {
    liveNodes.toSeq.iterator
  }

  override def onRemove(host: Host) = { }
  
  override def onUp(host: Host) = { }
  
  override def onDown(host: Host) = { }
  
  override def onSuspected(host: Host) = { }
}