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

import org.apache.accumulo.core.client.{ Connector => AccumuloConnector }
import org.apache.accumulo.core.data.{ Key => AccumuloKey }
import org.apache.accumulo.core.data.{ Range => AccumuloRange }
import com.tetra.accumulo_scala.util.ScannerProxy
import com.tetra.accumulo_scala.util.Auths
import com.tetra.accumulo_scala.util.CloseableIterator
import org.apache.hadoop.io.Text

/**
 * This class provides a transition from an AccumuloConnector to a ScannerProxy so we can scan nicely
 * 
 * Uses System Property com.tetra.accumulo_scala.auths as the default auths if not provided.
 */
class ConnectorOps(connector: AccumuloConnector) {
  def scan(table: String): ScannerProxy = {
    scan(table, System.getProperty("com.tetra.accumulo_scala.auths", ""))
  }

  def scan(table: String, auths: String): ScannerProxy = {
    scan(table, auths.split(","))
  }

  def scan(table: String, auths: Iterable[String]): ScannerProxy = {
    return new ScannerProxy(connector, Auths.getAuths(auths), table)
  }
}

/**
 * This companion object provides implicits to make things seamless
 */
object ConnectorOps {
  implicit def connector2Ops(connector: AccumuloConnector): ConnectorOps = {
    new ConnectorOps(connector)
  }

  implicit def key2Range(key: AccumuloKey): AccumuloRange = {
    new AccumuloRange(key, key)
  }

  implicit def string2Key(rowId: String): AccumuloKey = {
    new AccumuloKey(rowId)
  }

  implicit def string2Range(rowId: String): AccumuloRange = {
    new AccumuloRange(rowId)
  }
  
  implicit def iterator2CI[T](iter: Iterator[T]) = CloseableIterator.iterator2CloseableIterator(iter)
}