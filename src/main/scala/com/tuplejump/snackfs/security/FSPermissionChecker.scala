package com.tuplejump.snackfs.security

import scala.collection.JavaConversions._
import scala.concurrent.Await
import scala.concurrent.Future
import scala.concurrent.duration.FiniteDuration
import org.apache.commons.lang.StringUtils
import org.apache.hadoop.fs.Path
import org.apache.hadoop.fs.permission.FsAction
import org.apache.hadoop.fs.permission.FsPermission
import org.apache.hadoop.security.AccessControlException
import com.tuplejump.snackfs.cassandra.partial.FileSystemStore
import com.tuplejump.snackfs.fs.model.INode
import com.tuplejump.snackfs.util.LogConfiguration
import com.twitter.logging.Logger
import scala.util.Try
import scala.util.Success
import scala.util.Failure
import java.util.Collections

case class FSPermissionChecker(store: FileSystemStore, user: String, useACL: Boolean) {
  
  private lazy val log = Logger.get(getClass)
  
  val groups: java.util.Set[String] = if(useACL) UnixGroupsMapping.getUserGroups(user) else Collections.emptySet()
  
  def getUser: String = user
  
  def getGroups: java.util.Set[String] = groups
  
  def containsGroup(group: String): Boolean = {
    groups.contains(group)
  }

  def checkOwner(inode: INode): Unit = {
    if (useACL) {
      if (inode != null && inode.user.equals(user)) {
        return
      }
      throw new AccessControlException("Permission denied")
    }
  }
  
  def checkPermission(path: Path, access: FsAction, doCheckOwner: Boolean, checkAncestor: Boolean, checkChildren: Boolean, atMost: FiniteDuration): Unit = {
    Try(Await.result(store.retrieveINode(path), atMost)) match {
      case Success(inode: INode) =>
        checkPermission(path, inode, access, doCheckOwner, checkAncestor, checkChildren, atMost)
      case Failure(e: Throwable) =>
        log.error(Thread.currentThread.getName() + " Failed to check permission: " + e.getMessage)
    }
  }
  
  def checkPermission(path: Path, inode: INode, access: FsAction, doCheckOwner: Boolean, checkAncestor: Boolean, checkChildren: Boolean, atMost: FiniteDuration): Unit = {
    if(useACL) {
      if(LogConfiguration.isDebugEnabled()) log.debug("Checking permissions for %s", path)
      check(path, inode, access, doCheckOwner)
      if(checkAncestor) {
        traverseAncestors(path, access, doCheckOwner, atMost)
      }
      if(checkChildren) {
        traverseChildren(path, access, doCheckOwner, atMost)
      }
    }
  }
  
  private def check(path: Path, inode: INode, access: FsAction, doCheckOwner: Boolean): Unit = {
    if(inode == null) {
      return
    }
    if(doCheckOwner) {
      checkOwner(inode)
    }
    if(access != null) {
      val mode: FsPermission = inode.permission
      if(inode.user.equals(user)) {
        if(mode.getUserAction.implies(access)) {
          return
        }
      } else if(containsGroup(inode.group)) {
      	if(mode.getGroupAction.implies(access)) {
          return
        }
      } else {
        if(mode.getOtherAction.implies(access)) {
          return
        }
      }
      throw new AccessControlException(toAccessControlString(path, inode, access, mode))
    }
  }
    
  private def traverseAncestors(path: Path, access: FsAction, doCheckOwner: Boolean, atMost: FiniteDuration): Unit = {
    var parent = path.getParent
    var paths = List[Path]()
    while (parent != null) {
      paths = paths :+ parent
      parent = parent.getParent
    }
    var subPathInodeFutures: Seq[Tuple2[Path, Future[INode]]] = Nil
    paths.foreach { ancestorPath => 
      if(LogConfiguration.isDebugEnabled) log.debug(Thread.currentThread.getName() + " Checking path: %s", ancestorPath)
      subPathInodeFutures = subPathInodeFutures :+ (ancestorPath, store.retrieveINode(ancestorPath))
    }
    subPathInodeFutures.foreach { pair =>
      Try(Await.result(pair._2, atMost)) match {
        case Success(inode: INode) =>
          val path = pair._1
          check(path, inode, access, doCheckOwner)
          if (inode != null) { //we bail out after the first success
            return
          }
        case Failure(e: Throwable) =>
          log.error(Thread.currentThread.getName() + " Failed to check permission: " + e.getMessage)
      }
    }
  }
  
  private def traverseChildren(path: Path, access: FsAction, doCheckOwner: Boolean, atMost: FiniteDuration): Unit = {
    val subPaths: Set[Path] = Await.result(store.fetchSubPaths(path, true), atMost)
    var subPathInodeFutures: Seq[Tuple2[Path, Future[INode]]] = Nil
    subPaths.foreach { subPath => 
      if(LogConfiguration.isDebugEnabled) log.debug(Thread.currentThread.getName() + " Checking path: %s", subPath)
      subPathInodeFutures = subPathInodeFutures :+ (subPath, store.retrieveINode(subPath))
    }
    subPathInodeFutures.foreach { pair =>
      Try(Await.result(pair._2, atMost)) match {
        case Success(inode: INode) =>
          val path = pair._1
          check(path, inode, access, doCheckOwner)
        case Failure(e: Throwable) =>
          log.error(Thread.currentThread.getName() + " Failed to check permission: " + e.getMessage)
      }
    }
  }
  
  private def getPathNames(path: String): Array[String] = {
    if (path == null || !path.startsWith(Path.SEPARATOR)) {
      throw new AssertionError("Absolute path required")
    }
    StringUtils.split(path, Path.SEPARATOR_CHAR)
  }

  private def constructPath(components: Array[String], start: Int, end: Int): String = {
    val buf = new StringBuilder
    for (i <- start until end) {
      buf.append(components(i))
      if (i < end - 1) {
        buf.append(Path.SEPARATOR)
      }
    }
    buf.toString
  }

  private def toAccessControlString(path: Path, inode: INode, access: FsAction, mode: FsPermission): String = {
    val sb = new StringBuilder("Permission denied: ").append("user=")
      .append(user)
      .append(", ")
      .append("access=")
      .append(access)
      .append(", ")
      .append("inode=\"")
      .append(path.toUri.getPath)
      .append("\":")
      .append(inode.user)
      .append(':')
      .append(inode.group)
      .append(':')
      .append(if (inode.isDirectory) 'd' else '-')
      .append(mode)
    sb.toString
  }
  
}