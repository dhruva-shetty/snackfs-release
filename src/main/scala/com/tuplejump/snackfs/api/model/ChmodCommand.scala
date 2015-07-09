package com.tuplejump.snackfs.api.model

import java.io.IOException
import scala.concurrent.Await
import scala.concurrent.duration.FiniteDuration
import scala.util.Failure
import scala.util.Success
import scala.util.Try
import org.apache.hadoop.fs.FSDataInputStream
import org.apache.hadoop.fs.Path
import com.tuplejump.snackfs.api.partial.Command
import com.tuplejump.snackfs.cassandra.partial.FileSystemStore
import com.tuplejump.snackfs.fs.model.INode
import com.tuplejump.snackfs.fs.stream.FileSystemInputStream
import com.tuplejump.snackfs.util.LogConfiguration
import com.twitter.logging.Logger
import org.apache.hadoop.fs.permission.FsPermission
import org.apache.hadoop.fs.permission.FsAction

object ChmodCommand extends Command {

  private lazy val log = Logger.get(getClass)

  def apply(store: FileSystemStore, filePath: Path, newPermission: FsPermission, atMost: FiniteDuration): Unit = {
    val mayBeFile = Try(Await.result(store.retrieveINode(filePath), atMost))
    
    mayBeFile match {
      case Success(inode: INode) =>
        log.info(Thread.currentThread.getName() + " chmod path %s from %s to %s", filePath, inode.permission, newPermission)
        var newInode = new INode(inode.user, inode.group, newPermission, inode.fileType, inode.blocks, inode.timestamp) 
  
        //need to check write permission and also if we are the owner of the file
        store.permissionChecker.checkPermission(filePath, inode, null, true, false, false, atMost)
        
        log.info(Thread.currentThread.getName() + " deleting existing iNode %s", filePath)
        Await.ready(store.deleteINode(filePath), atMost)
        
        log.info(Thread.currentThread.getName() + " storing iNode %s", filePath)
        Await.ready(store.storeINode(filePath, newInode), atMost)

      case Failure(e: Throwable) =>
        val ex = new IOException("No such file.")
        log.error(ex, "Failed to do chmod for file %s as it doesnt exist", filePath)
        throw ex
    }
  }
}