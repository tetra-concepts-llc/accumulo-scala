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

import org.apache.accumulo.core.client.{Connector => AccumuloConnector}
import org.apache.accumulo.core.data.{Key => AccumuloKey}
import org.apache.accumulo.core.data.{Range => AccumuloRange}
import org.apache.accumulo.core.security.{Authorizations => AccumuloAuthorizations}
import org.apache.hadoop.io.Text

/**
 * config of a ScannerProxy
 * 
 * Uses System Property com.tetra.accumulo_scala.strict to suppress exceptions and just return empty iterators.
 */
protected[util] trait ScannerProxyConfig {
  val conn: AccumuloConnector
  val auths: AccumuloAuthorizations
  val tableName: String
  var fromKey: Option[AccumuloKey] = None
  var toKey: Option[AccumuloKey] = None
  var ranges: Option[CloseableIterator[AccumuloRange]] = None
  var familyQualifiers: Option[List[(Text, Text)]] = None
  var isStrict: Boolean = System.getProperty("com.tetra.accumulo_scala.strict", "false").toBoolean
}