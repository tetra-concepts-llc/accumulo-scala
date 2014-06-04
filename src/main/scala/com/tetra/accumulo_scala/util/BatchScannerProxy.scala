/* ********************************************************** *\
**    ___________     __                 
**    \__    ___/____/  |_____________   
**      |    |_/ __ \   __\_  __ \__  \    Tetra Concepts LLC
**      |    |\  ___/|  |  |  | \// __ \_  tetraconcepts.com
**      |____| \___  >__|  |__|  (____  /
**                 \/                 \/ 
\* ********************************************************** */
/*
 * Copyright (C) 2014 Tetra Concepts LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.tetra.accumulo_scala.util

import java.util.Map.Entry
import java.util.concurrent.TimeUnit
import scala.collection.JavaConversions.asJavaIterator
import scala.collection.JavaConversions.asScalaIterator
import scala.collection.JavaConversions.seqAsJavaList
import org.apache.accumulo.core.client.BatchScanner
import org.apache.accumulo.core.client.IteratorSetting
import org.apache.accumulo.core.client.TableNotFoundException
import org.apache.accumulo.core.data.{ Key => AccumuloKey }
import org.apache.accumulo.core.data.{ Range => AccumuloRange }
import org.apache.accumulo.core.data.{ Value => AccumuloValue }
import org.apache.commons.logging.LogFactory
import org.apache.hadoop.io.Text
import org.apache.commons.io.IOUtils

/**
 * Use a BatchScanner to get the info from accumulo
 */
class BatchScannerProxy(config: ScannerProxyConfig, numQueryThreads: Int, ranges: CloseableIterator[AccumuloRange]) extends CloseableIterator[Entry[AccumuloKey, AccumuloValue]] {
  private val LOG = LogFactory.getLog(classOf[BatchScannerProxy])

  private val rangeGroups = ranges.grouped(500)
  private var batchScanner: BatchScanner = BatchScannerProxy.emptyBatchScanner()
  private var iter: Iterator[Entry[AccumuloKey, AccumuloValue]] = Iterator.empty

  private var closed = false

  /**
   * {@inheritDoc}
   */
  override def hasNext(): Boolean = {
    if (closed) return false

    try {
      if (iter.hasNext) return true

      try {
        while (!iter.hasNext && rangeGroups.hasNext) {
          batchScanner.close()

          val bScanner = config.conn.createBatchScanner(config.tableName, config.auths, numQueryThreads)
          bScanner.setRanges(rangeGroups.next)
          if (config.familyQualifiers.isDefined) {
            config.familyQualifiers.get.foreach { fq =>
              if (fq._2 == null) {
                bScanner.fetchColumnFamily(fq._1)
              } else {
                bScanner.fetchColumn(fq._1, fq._2)
              }
            }
          }

          batchScanner = bScanner
          iter = bScanner.iterator
        }
      } catch {
        case tnfe: TableNotFoundException => {
          LOG.error(tnfe)
          close
          if (config.isStrict) throw tnfe
        }
        case e: Exception => {
          LOG.error(e)
          close
          if (config.isStrict) throw e
        }
      }

      if (iter.hasNext) return true
    } catch {
      case t: Throwable => {
        close()
        throw t
      }
    }

    close()
    false
  }

  /**
   * {@inheritDoc}
   */
  override def next(): Entry[AccumuloKey, AccumuloValue] = {
    try {
      if (!hasNext) Iterator.empty.next

      iter.next
    } catch {
      case t: Throwable => {
        close()
        throw t
      }
    }
  }

  /**
   * {@inheritDoc}
   */
  override def close(): Unit = {
    if(!closed) {
    	closeQuietly(batchScanner)
    	IOUtils.closeQuietly(ranges)
    }
    
    closed = true
  }
  
  /*
   * if only the batchScanner was just closeable ...
   */
  private def closeQuietly(_batchScanner: BatchScanner): Unit = {
    try {
      _batchScanner.close()
    } catch {
      case _: Throwable => { /*shh*/ }
    }
  }
}

object BatchScannerProxy {
  def empty() = new BatchScannerProxy(null, -1, null) {
    override def hasNext(): Boolean = false
    override def next(): Entry[AccumuloKey, AccumuloValue] = Iterator.empty.next
    override def close(): Unit = {}
  }

  def emptyBatchScanner() = new BatchScanner {
    override def addScanIterator(cfg: IteratorSetting): Unit = {}
    override def clearColumns(): Unit = {}
    override def clearScanIterators(): Unit = {}
    override def close(): Unit = {}
    override def fetchColumn(colFam: Text, colQual: Text): Unit = {}
    override def fetchColumnFamily(colFam: Text): Unit = {}
    override def getTimeout(timeUnit: TimeUnit): Long = { -1L }
    override def iterator(): java.util.Iterator[Entry[AccumuloKey, AccumuloValue]] = Iterator.empty
    override def removeScanIterator(iteratorName: String): Unit = {}
    override def setRanges(ranges: java.util.Collection[AccumuloRange]): Unit = {}
    override def setTimeout(timeout: Long, timeUnit: TimeUnit): Unit = {}
    override def updateScanIteratorOption(iteratorName: String, key: String, value: String): Unit = {}
  }
}