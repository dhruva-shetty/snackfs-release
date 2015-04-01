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
package com.tuplejump.snackfs.cassandra.partial

import java.io.InputStream
import java.nio.ByteBuffer
import java.util.UUID
import scala.concurrent.Future
import org.apache.hadoop.fs.Path
import com.tuplejump.snackfs.cassandra.model.GenericOpSuccess
import com.tuplejump.snackfs.cassandra.model.Keyspace
import com.tuplejump.snackfs.fs.model.BlockMeta
import com.tuplejump.snackfs.fs.model.INode
import com.tuplejump.snackfs.fs.model.SubBlockMeta
import com.tuplejump.snackfs.cassandra.model.SnackFSConfiguration
import com.tuplejump.snackfs.security.FSPermissionChecker
import com.tuplejump.snackfs.server.SnackFSClient
import com.tuplejump.snackfs.cassandra.sstable.DirectSSTableReader

trait FileSystemStore {
  
  def config: SnackFSConfiguration

  def createKeyspace: Future[Keyspace]

  def init: Unit

  def storeINode(path: Path, iNode: INode): Future[GenericOpSuccess]

  def retrieveINode(path: Path): Future[INode]

  def storeSubBlock(blockId: UUID, subBlockMeta: SubBlockMeta, data: ByteBuffer, compressed: Boolean): Future[GenericOpSuccess]

  def retrieveSubBlock(blockId: UUID, subBlockId: UUID, compressed: Boolean): Future[InputStream]

  def retrieveBlock(blockMeta: BlockMeta, compressed: Boolean, isLocalBlock: Boolean): InputStream

  def deleteINode(path: Path): Future[GenericOpSuccess]

  def deleteBlocks(iNode: INode): Future[GenericOpSuccess]

  def fetchSubPaths(path: Path, isDeepFetch: Boolean): Future[Set[Path]]

  def getBlockLocations(path: Path): Future[Map[BlockMeta, List[String]]]

  def acquireFileLock(path:Path,processId:UUID):Future[Boolean]

  def releaseFileLock(path:Path):Future[Boolean]
  
  def permissionChecker: FSPermissionChecker
  
  def getRemoteActor: SnackFSClient
  
  def getSSTableReader: DirectSSTableReader
}
