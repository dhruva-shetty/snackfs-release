package com.tuplejump.snackfs

import java.net.URI
import java.util.EnumSet

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.AbstractFileSystem
import org.apache.hadoop.fs.BlockLocation
import org.apache.hadoop.fs.CreateFlag
import org.apache.hadoop.fs.FSDataInputStream
import org.apache.hadoop.fs.FSDataOutputStream
import org.apache.hadoop.fs.FileChecksum
import org.apache.hadoop.fs.FileStatus
import org.apache.hadoop.fs.FsServerDefaults
import org.apache.hadoop.fs.FsStatus
import org.apache.hadoop.fs.Options.ChecksumOpt
import org.apache.hadoop.fs.Path
import org.apache.hadoop.fs.local.LocalConfigKeys
import org.apache.hadoop.fs.permission.FsPermission
import org.apache.hadoop.util.Progressable

import com.tuplejump.snackfs.util.LogConfiguration
import com.twitter.logging.Logger

class SnackFSDelegate(uri: URI, configuration: Configuration) extends AbstractFileSystem(uri, uri.getScheme, false, uri.getPort) {
  
  private lazy val log = Logger.get(getClass)
  
  private val snackFS: SnackFS = {
    val fs = new SnackFS
    fs.initialize(uri, configuration)
    fs
  }
  
  override def createInternal(f: Path,  flag: EnumSet[CreateFlag], absolutePermission: FsPermission, bufferSize: Int,  replication: Short, blockSize: Long, progress: Progressable, checksumOpt: ChecksumOpt, createParent: Boolean): FSDataOutputStream = snackFS.create(f)

  override def delete(f: Path, recursive: Boolean): Boolean = snackFS.delete(f, recursive)

  override def getFileBlockLocations(f: Path, start: Long, len: Long): Array[BlockLocation] = snackFS.getFileBlockLocations(f, start, len)

  override def getFileChecksum(f: Path): FileChecksum = snackFS.getFileChecksum(f)

  override def getFileStatus(f: Path): FileStatus = snackFS.getFileStatus(f)

  override def getFsStatus: FsStatus = new FsStatus(java.lang.Long.MAX_VALUE, 0, java.lang.Long.MAX_VALUE)

  override def getServerDefaults: FsServerDefaults = LocalConfigKeys.getServerDefaults

  override def getUriDefaultPort: Int = snackFS.getConfiguration.CassandraThriftPort

  override def listStatus(f: Path): Array[FileStatus] = snackFS.listStatus(f)

  override def mkdir(dir: Path, permission: FsPermission, createParent: Boolean):Unit = snackFS.mkdirs(dir)

  override def open(f: Path, bufferSize: Int): FSDataInputStream = snackFS.open(f, bufferSize)

  override def renameInternal(src: Path, dst: Path):Unit = snackFS.rename(src, dst)

  override def setOwner(f: Path, username: String, group: String):Unit = snackFS.setOwner(f, username, group)

  override def setPermission(f: Path, permission: FsPermission):Unit = snackFS.setPermission(f, permission)

  override def setReplication(f: Path, replication: Short): Boolean = snackFS.setReplication(f, replication)

  override def setTimes(f: Path, mtime: Long, atime: Long):Unit = snackFS.setTimes(f, mtime, atime)

  override def setVerifyChecksum(verifyChecksum: Boolean):Unit = snackFS.setVerifyChecksum(verifyChecksum)
  
  override def resolvePath(p: Path): Path = {
    checkPath(p)
    if (p.isAbsolute && p.toUri.getScheme == null && p.toUri.getAuthority == null) {
      getFileStatus(new Path(snackFS.getUri.getScheme, snackFS.getUri.getAuthority, p.toUri.getPath)).getPath
    } else {
      getFileStatus(p).getPath
    }
  }
}

