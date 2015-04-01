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

import scala.concurrent.Future
import scala.concurrent.Promise
import org.apache.thrift.async.AsyncMethodCallback
import scala.concurrent.ExecutionContextExecutor
import scala.concurrent.ExecutionContext
import java.util.concurrent.ThreadPoolExecutor
import java.util.concurrent.TimeUnit
import java.util.concurrent.LinkedBlockingQueue
import com.twitter.logging.Logger
import com.tuplejump.snackfs.cassandra.compression.LZ4CompressorInstance
import java.nio.ByteBuffer
import org.apache.cassandra.utils.ByteBufferUtil

object AsyncUtil {
  
  private lazy val log = Logger.get(getClass)

  val executor: ExecutionContextExecutor = ExecutionContext.fromExecutor(new ThreadPoolExecutor(16, 32, 4, TimeUnit.MINUTES, new LinkedBlockingQueue))

  def getExecutionContext(): ExecutionContextExecutor = {
    executor
  }

  /**
   * A method that takes in a (partially applied) method which takes AsyncMethodCallback
   * and invokes it on completion or failure.
   *
   * @param f
   * @tparam T
   * @return
   */
  def executeAsync[T](f: AsyncMethodCallback[T] => Unit): Future[T] = {

    class PromisingHandler extends AsyncMethodCallback[T] {
      val p = Promise[T]()

      def onComplete(p1: T) {
        p success p1
      }

      def onError(p1: Exception) {
        p failure p1
      }
    }

    val promisingHandler: PromisingHandler = new PromisingHandler()

    f(promisingHandler)

    promisingHandler.p.future
  }
  
  @annotation.tailrec
  def retry[T](n: Int)(fn: => T): T = {
    util.Try { fn } match {
      case util.Success(x) => x
      case _ if n > 1 => { log.info("Retrying %s time(s)", n-1); retry(n - 1)(fn) }
      case util.Failure(e) => throw e
    }
  }
  
  def convertByteArrayToStream(bytes: Array[Byte], compressed: Boolean) = ByteBufferUtil.inputStream(ByteBuffer.wrap(LZ4CompressorInstance.decompress(bytes, compressed)))
}
