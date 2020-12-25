/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.storage

import java.io.DataInputStream
import java.nio.ByteBuffer

import scala.concurrent.Future
import scala.reflect.ClassTag

import org.apache.hadoop.fs.{FileSystem, Path}

import org.apache.spark.SparkConf
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.internal.Logging
import org.apache.spark.internal.config.STORAGE_DECOMMISSION_FALLBACK_STORAGE_PATH
import org.apache.spark.network.buffer.{ManagedBuffer, NioManagedBuffer}
import org.apache.spark.rpc.{RpcAddress, RpcEndpointRef, RpcTimeout}
import org.apache.spark.shuffle.{IndexShuffleBlockResolver, ShuffleBlockInfo}
import org.apache.spark.shuffle.IndexShuffleBlockResolver.NOOP_REDUCE_ID
import org.apache.spark.util.Utils

/**
 * A fallback storage used by storage decommissioners.
 */
private[storage] class FallbackStorage(conf: SparkConf) extends Logging {
  require(conf.contains("spark.app.id"))
  require(conf.get(STORAGE_DECOMMISSION_FALLBACK_STORAGE_PATH).isDefined)

  private val fallbackPath = new Path(conf.get(STORAGE_DECOMMISSION_FALLBACK_STORAGE_PATH).get)
  private val hadoopConf = SparkHadoopUtil.get.newConfiguration(conf)
  private val fallbackFileSystem = FileSystem.get(fallbackPath.toUri, hadoopConf)
  private val appId = conf.getAppId

  // Visible for testing
  def copy(
      shuffleBlockInfo: ShuffleBlockInfo,
      bm: BlockManager): Unit = {
    val shuffleId = shuffleBlockInfo.shuffleId
    val mapId = shuffleBlockInfo.mapId

    bm.migratableResolver match {
      case r: IndexShuffleBlockResolver =>
        val indexFile = r.getIndexFile(shuffleId, mapId)

        if (indexFile.exists()) {
          fallbackFileSystem.copyFromLocalFile(
            new Path(indexFile.getAbsolutePath),
            new Path(fallbackPath, s"$appId/$shuffleId/${indexFile.getName}"))

          val dataFile = r.getDataFile(shuffleId, mapId)
          if (dataFile.exists()) {
            fallbackFileSystem.copyFromLocalFile(
              new Path(dataFile.getAbsolutePath),
              new Path(fallbackPath, s"$appId/$shuffleId/${dataFile.getName}"))
          }

          // Report block statuses
          val reduceId = NOOP_REDUCE_ID
          val indexBlockId = ShuffleIndexBlockId(shuffleId, mapId, reduceId)
          FallbackStorage.reportBlockStatus(bm, indexBlockId, indexFile.length)
          if (dataFile.exists) {
            val dataBlockId = ShuffleDataBlockId(shuffleId, mapId, reduceId)
            FallbackStorage.reportBlockStatus(bm, dataBlockId, dataFile.length)
          }
        }
      case r =>
        logWarning(s"Unsupported Resolver: ${r.getClass.getName}")
    }
  }

  def exists(shuffleId: Int, filename: String): Boolean = {
    fallbackFileSystem.exists(new Path(fallbackPath, s"$appId/$shuffleId/$filename"))
  }
}

class NoopRpcEndpointRef(conf: SparkConf) extends RpcEndpointRef(conf) {
  import scala.concurrent.ExecutionContext.Implicits.global
  override def address: RpcAddress = null
  override def name: String = "fallback"
  override def send(message: Any): Unit = {}
  override def ask[T: ClassTag](message: Any, timeout: RpcTimeout): Future[T] = {
    Future{true.asInstanceOf[T]}
  }
}

object FallbackStorage extends Logging {
  /** We use one block manager id as a place holder. */
  val FALLBACK_BLOCK_MANAGER_ID: BlockManagerId = BlockManagerId("fallback", "remote", 7337)

  def getFallbackStorage(conf: SparkConf): Option[FallbackStorage] = {
    if (conf.get(STORAGE_DECOMMISSION_FALLBACK_STORAGE_PATH).isDefined) {
      Some(new FallbackStorage(conf))
    } else {
      None
    }
  }

  /** Register the fallback block manager and its RPC endpoint. */
  def registerBlockManagerIfNeeded(master: BlockManagerMaster, conf: SparkConf): Unit = {
    if (conf.get(STORAGE_DECOMMISSION_FALLBACK_STORAGE_PATH).isDefined) {
      master.registerBlockManager(
        FALLBACK_BLOCK_MANAGER_ID, Array.empty[String], 0, 0, new NoopRpcEndpointRef(conf))
    }
  }

  /** Report block status to block manager master and map output tracker master. */
  private def reportBlockStatus(blockManager: BlockManager, blockId: BlockId, dataLength: Long) = {
    assert(blockManager.master != null)
    blockManager.master.updateBlockInfo(
      FALLBACK_BLOCK_MANAGER_ID, blockId, StorageLevel.DISK_ONLY, memSize = 0, dataLength)
  }

  /**
   * Read a ManagedBuffer.
   */
  def read(conf: SparkConf, blockId: BlockId): ManagedBuffer = {
    logInfo(s"Read $blockId")
    val fallbackPath = new Path(conf.get(STORAGE_DECOMMISSION_FALLBACK_STORAGE_PATH).get)
    val hadoopConf = SparkHadoopUtil.get.newConfiguration(conf)
    val fallbackFileSystem = FileSystem.get(fallbackPath.toUri, hadoopConf)
    val appId = conf.getAppId

    val (shuffleId, mapId, startReduceId, endReduceId) = blockId match {
      case id: ShuffleBlockId =>
        (id.shuffleId, id.mapId, id.reduceId, id.reduceId + 1)
      case batchId: ShuffleBlockBatchId =>
        (batchId.shuffleId, batchId.mapId, batchId.startReduceId, batchId.endReduceId)
      case _ =>
        throw new IllegalArgumentException("unexpected shuffle block id format: " + blockId)
    }

    val name = ShuffleIndexBlockId(shuffleId, mapId, NOOP_REDUCE_ID).name
    val indexFile = new Path(fallbackPath, s"$appId/$shuffleId/$name")
    val start = startReduceId * 8L
    val end = endReduceId * 8L
    Utils.tryWithResource(fallbackFileSystem.open(indexFile)) { inputStream =>
      Utils.tryWithResource(new DataInputStream(inputStream)) { index =>
        index.skip(start)
        val offset = index.readLong()
        index.skip(end - (start + 8L))
        val nextOffset = index.readLong()
        val name = ShuffleDataBlockId(shuffleId, mapId, NOOP_REDUCE_ID).name
        val dataFile = new Path(fallbackPath, s"$appId/$shuffleId/$name")
        val f = fallbackFileSystem.open(dataFile)
        val size = nextOffset - 1 - offset
        logDebug(s"To byte array $size")
        val array = new Array[Byte](size.toInt)
        val startTimeNs = System.nanoTime()
        f.seek(offset)
        f.read(array)
        logDebug(s"Took ${(System.nanoTime() - startTimeNs) / (1000 * 1000)}ms")
        f.close()
        new NioManagedBuffer(ByteBuffer.wrap(array))
      }
    }
  }
}

