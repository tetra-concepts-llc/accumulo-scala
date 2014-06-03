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
import java.util.Iterator
import java.util.Map.Entry
import org.apache.accumulo.core.data.Key
import org.apache.accumulo.core.data.Value
import java.lang.Throwable

@RunWith(classOf[JUnitRunner])
class ScannerProxySpec extends UnitSpec {
  def fixture =
    new {
      val tableName = "xyz"
      val conn = new MockInstance(UUID.randomUUID().toString).getConnector("", "")
      conn.tableOperations().create(tableName)

      //setup
      private val _a = new Mutation("a")
      _a.put("fa", "qa", "v")
      private val _h = new Mutation("h")
      _h.put("fh", "qh", new ColumnVisibility("hidden"), "v")
      private val _k = new Mutation("k")
      _k.put("fk", "qk1", "v")
      _k.put("fk", "qk2", "v")
      private val _z = new Mutation("z")
      _z.put("fz", "qz", "v")
      private val _writer = conn.createBatchWriter(tableName, new BatchWriterConfig)
      _writer.addMutations(List(_a, _h, _k, _z))
      _writer.flush()
      _writer.close()
    }

  "A ScannerPoxy" should "throw an exception if the table does not exist if strict" in {
    val conn = new MockInstance(UUID.randomUUID().toString).getConnector("", "")
    val scanner = new ScannerProxy(conn, Auths.EMPTY, "abc")
    scanner.strict(true)

    intercept[TableNotFoundException] {
      scanner.hasNext
    }
  }

  it should "return an empty iterator if strict is set to false" in {
    val conn = new MockInstance(UUID.randomUUID().toString).getConnector("", "")
    val scanner = new ScannerProxy(conn, Auths.EMPTY, "abc")
    scanner.strict(false)

    assert(!scanner.hasNext)
  }

  it should "do a full table scan if no range parameters are set" in {
    val f = fixture
    val sp = new ScannerProxy(f.conn, Auths.EMPTY, f.tableName)

    assert("akkz" == sp.foldLeft("")((rows, e) => rows + e.getKey().getRow().toString()))
  }

  it should "skip a if starting at b" in {
    val f = fixture
    val sp = new ScannerProxy(f.conn, Auths.EMPTY, f.tableName)
    sp.from("b")

    assert("kkz" == sp.foldLeft("")((rows, e) => rows + e.getKey().getRow().toString()))
  }

  it should "skip z if ending at y" in {
    val f = fixture
    val sp = new ScannerProxy(f.conn, Auths.EMPTY, f.tableName)
    sp.to("y")

    assert("akk" == sp.foldLeft("")((rows, e) => rows + e.getKey().getRow().toString()))
  }

  it should "get a and z if provided in ranges" in {
    val f = fixture
    val sp = new ScannerProxy(f.conn, Auths.EMPTY, f.tableName)
    sp.in(List(new AccumuloRange("a"), new AccumuloRange("z")).iterator)

    assert("az" == sp.foldLeft("")((rows, e) => rows + e.getKey().getRow().toString()))
  }

  it should "used both ranges and to and from" in {
    val f = fixture
    val sp = new ScannerProxy(f.conn, Auths.EMPTY, f.tableName)
    sp.from("b").to("y")
    sp.in(List(new AccumuloRange("a"), new AccumuloRange("z")).iterator)

    assert("akkz" == (sp.foldLeft("")((rows, e) => rows + e.getKey().getRow().toString())).sorted)
  }

  it should "get only a if take(1)" in {
    val f = fixture
    val sp = new ScannerProxy(f.conn, Auths.EMPTY, f.tableName)

    assert("a" == sp.take(1).foldLeft("")((rows, e) => rows + e.getKey().getRow().toString()))
  }

  it should "get only k if drop(1) take(1)" in {
    val f = fixture
    val sp = new ScannerProxy(f.conn, Auths.EMPTY, f.tableName)

    assert("k" == sp.drop(1).take(1).foldLeft("")((rows, e) => rows + e.getKey().getRow().toString()))
  }

  it should "get only z if drop(N-1)" in {
    val f = fixture
    val sp = new ScannerProxy(f.conn, Auths.EMPTY, f.tableName)

    assert("z" == sp.drop(3).foldLeft("")((rows, e) => rows + e.getKey().getRow().toString()))
  }

  it should "get only kk if filtering on family fk" in {
    val f = fixture
    val sp = new ScannerProxy(f.conn, Auths.EMPTY, f.tableName)
    sp.filter("fk")

    assert("kk" == sp.foldLeft("")((rows, e) => rows + e.getKey().getRow().toString()))
  }

  it should "get only k if filtering on family fk qualifier qk2" in {
    val f = fixture
    val sp = new ScannerProxy(f.conn, Auths.EMPTY, f.tableName)
    sp.filter("fk:qk2")

    assert("k" == sp.foldLeft("")((rows, e) => rows + e.getKey().getRow().toString()))
  }

  it should "everything if we have the auths" in {
    val f = fixture
    val sp = new ScannerProxy(f.conn, Auths.getAuths("hidden"), f.tableName)

    assert("ahkkz" == sp.foldLeft("")((rows, e) => rows + e.getKey().getRow().toString()))
  }

  it should "close the ranges when it closes" in {
    val ranges = mock[CloseableIterator[Range]]

    expecting {
      ranges.close
    }

    val sp = new ScannerProxy(null, null, null)
    sp.in(ranges)

    whenExecuting(ranges) {
      sp.close
    }
  }

  it should "close the ranges when it throws an exception during hasNext" in {
    val conn = mock[Connector]
    val scanner = mock[Scanner]
    val iter = mock[Iterator[Entry[Key, Value]]]
    val ranges = mock[CloseableIterator[Range]]

    expecting {
      conn.createScanner(anyString(), anyObject()).andReturn(scanner)
      scanner.setRange(anyObject())
      scanner.iterator().andReturn(iter)
      iter.hasNext().andThrow(new RuntimeException).anyTimes
      ranges.hasNext().andReturn(true)
      ranges.next().andReturn(new Range)
      ranges.close.anyTimes()
    }

    val sp = new ScannerProxy(conn, Auths.EMPTY, "abc")
    sp.in(ranges)

    whenExecuting(conn, scanner, iter, ranges) {
      try {
        sp.hasNext
      } catch {
        case t: Throwable => { /*pass*/ }
      }
    }
  }

  it should "close the ranges when it throws an exception during next" in {
    val conn = mock[Connector]
    val scanner = mock[Scanner]
    val iter = mock[Iterator[Entry[Key, Value]]]
    val ranges = mock[CloseableIterator[Range]]

    expecting {
      conn.createScanner(anyString(), anyObject()).andReturn(scanner)
      scanner.setRange(anyObject())
      scanner.iterator().andReturn(iter)
      iter.hasNext().andReturn(true).anyTimes()
      iter.next().andThrow(new RuntimeException)
      ranges.hasNext().andReturn(true)
      ranges.next().andReturn(new Range)
      ranges.close.anyTimes()
    }

    val sp = new ScannerProxy(conn, Auths.EMPTY, "xyz")
    sp.in(ranges)

    whenExecuting(conn, scanner, iter, ranges) {
      try {
        sp.next
      } catch {
        case t: Throwable => { /*pass*/ }
      }
    }
  }
}