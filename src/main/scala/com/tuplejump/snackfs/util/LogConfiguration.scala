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
package com.tuplejump.snackfs.util

import java.nio.file.FileSystems
import java.nio.file.Files
import java.nio.file.LinkOption

import com.twitter.logging.FileHandler
import com.twitter.logging.Level
import com.twitter.logging.LoggerFactory
import com.twitter.logging.QueueingHandler

object LogConfiguration {
  
  val logdir = "/local1/logs/snackfs/"
  val logname = "snackfs.log"
  val loglevel = if (System.getenv("SNACKFS_LOG_LEVEL") != null) System.getenv("SNACKFS_LOG_LEVEL") else System.getProperty("SNACKFS_LOG_LEVEL")
  val asyncMaxSizeFlag = 4096

  val level = loglevel match {
    case "DEBUG" => Level.DEBUG
    case "INFO" => Level.INFO
    case "ERROR" => Level.ERROR
    case "ALL" => Level.ALL
    case "OFF" => Level.OFF
    case _ => Level.INFO
  }
  
  val config = new LoggerFactory("", Some(level), List(QueueingHandler(FileHandler(getLogLocation), asyncMaxSizeFlag)), false)
  
  val isDebug = level == Level.DEBUG
  
  def isDebugEnabled() : Boolean = isDebug
  
  def getLogLocation(): String = {
    val localDirectory = FileSystems.getDefault().getPath(logdir)
    if(Files.exists(localDirectory, LinkOption.NOFOLLOW_LINKS)){
      val path = FileSystems.getDefault().getPath(logdir, logname)
      if(!Files.exists(path, LinkOption.NOFOLLOW_LINKS)) {
        Files.createFile(path)
      }
      logdir + logname
    } else {
      val userHome = System.getProperty("user.home") + "/"
      val path = FileSystems.getDefault().getPath(userHome, logname)
      if(!Files.exists(path, LinkOption.NOFOLLOW_LINKS)) {
        Files.createFile(path)
      }
      userHome + logname
    }
  }
}
