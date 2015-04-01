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
package com.tuplejump.snackfs.api.model

import org.apache.hadoop.fs.FileStatus
import org.apache.hadoop.fs.Path

import com.tuplejump.snackfs.fs.model.INode
import com.twitter.logging.Logger

object SnackFileStatus {
  
  private lazy val log = Logger.get(getClass)
  
  def getLength(iNode: INode): Long = {
    var result = 0L
    try {
	    if (iNode.isFile) {
	      result = iNode.blocks.map(_.length).sum
	    }
	} catch {
		case e: Exception => log.error(e, "Failed to read length", iNode)
	}
    result
  }
  
  def getBlockSize(iNode: INode): Long = {
    var result = 0L
    try {
	    if (iNode.blocks != null && iNode.blocks.length > 0) {
	      result = iNode.blocks(0).length
	    }
    } catch {
      	case e: Exception => log.error(e, "Failed to read blockSize", iNode)
    }
    result
 }
}

case class SnackFileStatus(iNode: INode, path: Path) extends FileStatus(
  SnackFileStatus.getLength(iNode), //length
  iNode.isDirectory, //isDir
  0, //block_replication
  SnackFileStatus.getBlockSize(iNode), //blocksize
  iNode.timestamp, //modification_time
  0L, //access_time
  iNode.permission,
  iNode.user,
  iNode.group,
  path: Path)
