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
package com.tetra.accumulo_scala

import java.util.UUID
import scala.collection.JavaConversions.seqAsJavaList
import org.apache.accumulo.core.client.BatchWriterConfig
import org.apache.accumulo.core.client.mock.MockInstance
import org.apache.accumulo.core.data.Mutation
import org.apache.accumulo.core.security.ColumnVisibility
import com.tetra.accumulo_scala.ConnectorOps.connector2Ops
import org.apache.accumulo.core.data.Range
import com.tetra.accumulo_scala.ConnectorOps._
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class ConnectorOpsSpec extends UnitSpec {
  def fixture =
    new {
      val tableName = "xyz"
      val conn = new MockInstance(UUID.randomUUID().toString).getConnector("", "")
      conn.tableOperations().create(tableName)

      //setup
      private val _a = new Mutation("a")
      _a.put("f", "q", "v")
      private val _h = new Mutation("h")
      _h.put("f", "q", new ColumnVisibility("hidden"), "v")
      private val _k = new Mutation("k")
      _k.put("f", "q", "v")
      private val _z = new Mutation("z")
      _z.put("f", "q", "v")
      private val _writer = conn.createBatchWriter(tableName, new BatchWriterConfig)
      _writer.addMutations(List(_a, _h, _k, _z))
      _writer.flush()
      _writer.close()
    }

  "ConnectorOps" should "do a full table scan if no range parameters are set" in {
    val f = fixture
    val sp = f.conn scan "xyz"

    assert("akz" == sp.foldLeft("")((rows, e) => rows + e.getKey().getRow().toString()))
  }
  
  it should "a" in {
    val f = fixture
    val sp = f.conn scan "xyz" in ("a", "b")
  }
}