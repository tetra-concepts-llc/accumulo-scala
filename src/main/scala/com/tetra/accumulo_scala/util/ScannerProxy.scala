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
import scala.collection.JavaConversions._
import org.apache.accumulo.core.client.{ Connector => AccumuloConnector }
import org.apache.accumulo.core.client.{Scanner => AccumuloScanner}
import org.apache.accumulo.core.data.{ Key => AccumuloKey }
import org.apache.accumulo.core.data.{ Range => AccumuloRange }
import org.apache.accumulo.core.data.{ Value => AccumuloValue }
import org.apache.accumulo.core.security.{ Authorizations => AccumuloAuthorizations }
import scala.collection.mutable.ListBuffer
import org.apache.accumulo.core.client.TableNotFoundException
import org.apache.hadoop.io.Text
import org.apache.commons.logging.LogFactory

/**
 * Provides the DSL to scan an Accumulo table
 *
 * Uses System Property com.tetra.accumulo_scala.fq.delim to split a family from a qualifier when filtering
 */
class ScannerProxy(override val conn: AccumuloConnector, override val auths: AccumuloAuthorizations, override val tableName: String) extends CloseableIterator[Entry[AccumuloKey, AccumuloValue]] with ScannerProxyConfig {
  private val LOG = LogFactory.getLog(classOf[ScannerProxy])
  private val FQ_DELIM = System.getProperty("com.tetra.accumulo_scala.fq.delim", ":")

  private var doTableScan = true
  private var scanning = false
  private var closed = false
  private var iter: Iterator[Entry[AccumuloKey, AccumuloValue]] = Iterator.empty

  /**
   * Key to start scanning from
   */
  def from(f: AccumuloKey): ScannerProxy = {
    checkCanConfigure

    fromKey = Some(f)
    doTableScan = false

    this
  }

  /**
   * Key to scan to
   */
  def to(t: AccumuloKey): ScannerProxy = {
    checkCanConfigure

    toKey = Some(t)
    doTableScan = false

    this
  }

  /**
   * Ranges to scan
   */
  def in(ranges: String*): ScannerProxy = {
    in(ranges.map(new AccumuloRange(_)).iterator)
  }

  /**
   * Ranges to scan
   */
  def in(r: CloseableIterator[AccumuloRange]): ScannerProxy = {
    checkCanConfigure

    ranges = Some(r)
    doTableScan = false

    this
  }

  /**
   * Restrict the scan to only certain families/qualifiers
   */
  def filter(fqs: String*): ScannerProxy = {
    checkCanConfigure
    
    val f = { s: String =>
      if (s.contains(FQ_DELIM)) {
        val parts = s.split(FQ_DELIM)
        (new Text(parts(0)), new Text(parts(1)))
      } else (new Text(s), null)
    }

    familyQualifiers = Some(fqs.map(f).toList)

    this
  }

  /**
   * If there is an Accumulo Exception and strict is false, the exception will be logged
   * and an empty iterator will be returned
   */
  def strict(flag: Boolean): ScannerProxy = {
    checkCanConfigure

    isStrict = flag
    this
  }

  /**
   * Use a parallelized version of a Scanner
   */
  def par(numQueryThreads: Int): BatchScannerProxy = {
    checkCanConfigure

    var batchRanges: CloseableIterator[AccumuloRange] = CloseableIterator.empty

    if (ranges.isEmpty && toKey.isEmpty && fromKey.isEmpty) {
      //full table scan
      batchRanges = List(new AccumuloRange()).iterator
    } else {
      if (ranges.isDefined) {
        batchRanges = ranges.get
      }
      if (toKey.isDefined || fromKey.isDefined) {
        batchRanges = List(new AccumuloRange()).iterator ++ batchRanges
      }
    }

    new BatchScannerProxy(this, numQueryThreads, batchRanges)
  }

  /**
   * {@inheritDoc}
   */
  //TODO: simplify
  override def hasNext(): Boolean = {
    scanning = true

    if (closed) return false

    try {
      if (iter.hasNext) return true

      try {
        if (doTableScan) {
          doTableScan = false //only do once

          val scanner = getScanner
          iter = scanner.iterator()
          return iter.hasNext
        }

        if (toKey.isDefined || fromKey.isDefined) {
          val start = fromKey.getOrElse(null)
          val end = toKey.getOrElse(null)

          toKey = None
          fromKey = None

          val scanner = getScanner
          scanner.setRange(new AccumuloRange(start, end))
          iter = scanner.iterator()
          if (iter.hasNext) return true
        }

        while (!iter.hasNext && ranges.getOrElse(CloseableIterator.empty).hasNext) {
          val scanner = getScanner
          scanner.setRange(ranges.get.next)
          iter = scanner.iterator()
        }
      } catch {
        case tnfe: TableNotFoundException => {
          LOG.error(tnfe)
          close
          if (isStrict) throw tnfe
        }
        case e: Exception => {
          LOG.error(e)
          close
          if (isStrict) throw e
        }
      }

      if (iter.hasNext) return true
    } catch {
      case t: Throwable => {
        close
        throw t
      }
    }

    close()
    return false
  }

  /**
   * {@inheritDoc}
   */
  override def next(): Entry[AccumuloKey, AccumuloValue] = {
    scanning = true

    try {
      if (!hasNext) Iterator.empty.next

      iter.next
    } catch {
      case t: Throwable => {
        close
        throw t
      }
    }
  }

  /**
   * {@inheritDoc}
   */
  override def close(): Unit = {
    scanning = true
    closed = true
    ranges.getOrElse(CloseableIterator.empty).close
  }

  private def getScanner: AccumuloScanner = {
    val scanner = conn.createScanner(tableName, auths)

    if (familyQualifiers.isDefined) {
      familyQualifiers.get.foreach { fq =>
        if (fq._2 == null) {
          scanner.fetchColumnFamily(fq._1)
        } else {
          scanner.fetchColumn(fq._1, fq._2)
        }
      }
    }

    scanner
  }
  
  private def checkCanConfigure: Unit = {
    if (scanning) throw new IllegalArgumentException("already started scanning, can't configure now!")
    if (closed) throw new IllegalArgumentException("already closed, can't configure now!")
  }
}