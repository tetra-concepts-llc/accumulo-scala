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

import com.tetra.accumulo_scala.UnitSpec
import org.apache.accumulo.core.client.Connector
import org.apache.accumulo.core.security.Authorizations
import org.apache.accumulo.core.client.TableNotFoundException
import org.easymock.EasyMock._
import org.apache.accumulo.core.client.Scanner
import org.apache.accumulo.core.client.mock.MockConnector
import org.apache.accumulo.core.client.mock.MockInstance
import java.util.UUID
import org.apache.accumulo.core.client.BatchWriterConfig
import org.apache.accumulo.core.data.Mutation
import scala.collection.JavaConversions._
import com.tetra.accumulo_scala.ConnectorOps._
import org.apache.accumulo.core.data.{ Range => AccumuloRange }
import org.apache.accumulo.core.security.ColumnVisibility
import org.apache.accumulo.core.data.Range
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.apache.hadoop.io.Text
import java.util.Map.Entry
import org.apache.accumulo.core.data.Key
import org.apache.accumulo.core.data.Value
import org.apache.accumulo.core.client.BatchScanner

@RunWith(classOf[JUnitRunner])
class BatchScannerProxySpec extends UnitSpec {
  def fixture =
    new {
      val tableName = "xyz"
      val connector = new MockInstance(UUID.randomUUID().toString).getConnector("", "")
      connector.tableOperations().create(tableName)

      var conf = new ScannerProxyConfig {
        override val conn = connector
        override val auths = Auths.EMPTY
        override val tableName = "xyz"
      }

      //setup
      private val a = new Mutation("a")
      a.put("fa", "qa", "v")
      private val h = new Mutation("h")
      h.put("fh", "qh", new ColumnVisibility("hidden"), "v")
      private val k = new Mutation("k")
      k.put("fk", "qk1", "v")
      k.put("fk", "qk2", "v")
      private val z = new Mutation("z")
      z.put("fz", "qz", "v")
      private val writer = connector.createBatchWriter(tableName, new BatchWriterConfig)
      writer.addMutations(List(a, h, k, z))
      writer.flush()
      writer.close()
    }

  "A BatchScannerPoxy" should "throw an exception if the table does not exist if strict" in {
    val conf = new ScannerProxyConfig {
      override val conn = new MockInstance(UUID.randomUUID().toString).getConnector("", "")
      override val auths = Auths.EMPTY
      override val tableName = "abc"
      isStrict = true
    }
    val scanner = new BatchScannerProxy(conf, 2, List(new Range()).iterator)

    intercept[TableNotFoundException] {
      scanner.hasNext
    }
  }

  it should "return an empty iterator if strict is set to false" in {
    val conf = new ScannerProxyConfig {
      override val conn = new MockInstance(UUID.randomUUID().toString).getConnector("", "")
      override val auths = Auths.EMPTY
      override val tableName = "abc"
      isStrict = false
    }
    val scanner = new BatchScannerProxy(conf, 2, List(new Range()).iterator)

    assert(!scanner.hasNext)
  }

  it should "do a full table scan if no range parameters are set" in {
    val f = fixture
    val bsp = new BatchScannerProxy(f.conf, 2, List(new Range()).iterator)

    assert("akkz" == bsp.foldLeft("")((rows, e) => rows + e.getKey().getRow().toString()))
  }

  it should "skip a if starting at b" in {
    val f = fixture
    val bsp = new BatchScannerProxy(f.conf, 2, List(new Range("b", null)).iterator)

    assert("kkz" == bsp.foldLeft("")((rows, e) => rows + e.getKey().getRow().toString()))
  }

  it should "skip z if ending at y" in {
    val f = fixture
    val bsp = new BatchScannerProxy(f.conf, 2, List(new Range(null, "y")).iterator)

    assert("akk" == bsp.foldLeft("")((rows, e) => rows + e.getKey().getRow().toString()))
  }

  it should "get a and z if provided in ranges" in {
    val f = fixture
    val bsp = new BatchScannerProxy(f.conf, 2, List(new AccumuloRange("a"), new AccumuloRange("z")).iterator)

    assert("az" == bsp.foldLeft("")((rows, e) => rows + e.getKey().getRow().toString()))
  }

  it should "used both ranges and to and from" in {
    val f = fixture
    val bsp = new BatchScannerProxy(f.conf, 2, List(new AccumuloRange("a"), new AccumuloRange("z"), new AccumuloRange("b", "y")).iterator)

    assert("akkz" == (bsp.foldLeft("")((rows, e) => rows + e.getKey().getRow().toString())).sorted)
  }

  it should "get only 1 if take(1)" in {
    val f = fixture
    val bsp = new BatchScannerProxy(f.conf, 2, List(new Range()).iterator)

    assert(1 == bsp.take(1).foldLeft(0)((count, _) => count + 1))
  }

  it should "get only 1 if drop(1) take(1)" in {
    val f = fixture
    val bsp = new BatchScannerProxy(f.conf, 2, List(new Range()).iterator)

    assert(1 == bsp.drop(1).take(1).foldLeft(0)((count, _) => count + 1))
  }

  it should "get only 1 if drop(N-1)" in {
    val f = fixture
    val bsp = new BatchScannerProxy(f.conf, 2, List(new Range()).iterator)

    assert(1 == bsp.drop(3).foldLeft(0)((count, _) => count + 1))
  }

  it should "everything if we have the auths" in {
    val f = fixture
    f.conf = new ScannerProxyConfig {
      override val conn = f.conf.conn
      override val auths = Auths.getAuths("hidden")
      override val tableName = f.conf.tableName
    }
    val bsp = new BatchScannerProxy(f.conf, 2, List(new Range()).iterator)

    assert(5 == bsp.foldLeft(0)((count, _) => count + 1))
  }

  it should "get only kk if filtering on family fk" in {
    val f = fixture
    f.conf.familyQualifiers = Some(List((new Text("fk"), null)))
    val bsp = new BatchScannerProxy(f.conf, 2, List(new Range()).iterator)

    assert("kk" == bsp.foldLeft("")((rows, e) => rows + e.getKey().getRow().toString()))
  }

  it should "get only k if filtering on family fk qualifier qk2" in {
    val f = fixture
    f.conf.familyQualifiers = Some(List((new Text("fk"), new Text("qk2"))))
    val bsp = new BatchScannerProxy(f.conf, 2, List(new Range()).iterator)

    assert("k" == bsp.foldLeft("")((rows, e) => rows + e.getKey().getRow().toString()))
  }

  it should "close the ranges when it closes" in {
    val ranges = mock[CloseableIterator[Range]]

    expecting {
      ranges.close
    }

    val bsp = new ScannerProxy(null, null, null);
    bsp.in(ranges)

    whenExecuting(ranges) {
      bsp.close
    }
  }

  it should "close the ranges when it throws an exception during hasNext" in {
    val conn = mock[Connector]
    val scanner = mock[BatchScanner]
    val iter = mock[Iterator[Entry[Key, Value]]]

    //FIXME: how do I mock a GroupedIterator?
    val ranges = new CloseableIterator[Range] {
      val i = List(new Range()).iterator
      var closed = false

      def hasNext() = i.hasNext
      def next() = i.next
      def close() = {
        closed = true
      }
    }

    expecting {
      conn.createBatchScanner(anyString(), anyObject(), anyObject()).andReturn(scanner)
      scanner.setRanges(anyObject())
      scanner.iterator().andReturn(iter)
      iter.hasNext().andThrow(new RuntimeException)
    }

    val proxy = new ScannerProxy(conn, Auths.EMPTY, "abc")
    val sp = new BatchScannerProxy(proxy, 2, ranges)

    whenExecuting(conn, scanner, iter) {
      try {
        sp.hasNext
      } catch {
        case t: Throwable => {
          assert(ranges.closed)
        }
      }
    }
  }

  it should "close the ranges when it throws an exception during next" in {
    val conn = mock[Connector]
    val scanner = mock[BatchScanner]
    val iter = mock[Iterator[Entry[Key, Value]]]

    //FIXME: how do I mock a GroupedIterator?
    val ranges = new CloseableIterator[Range] {
      val i = List(new Range()).iterator
      var closed = false

      def hasNext() = i.hasNext
      def next() = i.next
      def close() = { closed = true }
    }

    expecting {
      conn.createBatchScanner(anyString(), anyObject(), anyObject()).andReturn(scanner)
      scanner.setRanges(anyObject())
      scanner.iterator().andReturn(iter)
      iter.hasNext().andReturn(true).anyTimes()
      iter.next().andThrow(new RuntimeException)
    }

    val proxy = new ScannerProxy(conn, Auths.EMPTY, "abc")
    val sp = new BatchScannerProxy(proxy, 2, ranges)

    whenExecuting(conn, scanner, iter) {
      try {
        sp.next
      } catch {
        case t: Throwable => {
          assert(ranges.closed)
        }
      }
    }
  }
}