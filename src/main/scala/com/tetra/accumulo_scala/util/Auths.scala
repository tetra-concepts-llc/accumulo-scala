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

import scala.Array.canBuildFrom
import scala.collection.JavaConversions._

import org.apache.accumulo.core.security.{Authorizations => AccumuloAuthorizations}

import com.google.common.base.Strings
import com.google.common.cache.CacheBuilder
import com.google.common.cache.CacheLoader

/**
 * A Helper to build AccumuloAuthorizations
 *
 * Uses System Property com.tetra.accumulo_scala.auths.delim to break a string into
 *   a list of auths
 */
object Auths {
  private val DELIM = System.getProperty("com.tetra.accumulo_scala.auths.delim", ",")
  val EMPTY = new AccumuloAuthorizations /* to make compatable with accumulo 1.5 */

  private val CACHE = CacheBuilder.newBuilder().maximumSize(100).build(new CacheLoader[String, AccumuloAuthorizations] {
    override def load(auths: String): AccumuloAuthorizations = {
      new AccumuloAuthorizations(auths.split(DELIM).map { s => s.trim().getBytes() }.toList)
    }
  })

  def getAuths(auths: Iterable[String]): AccumuloAuthorizations = {
    if (auths == null || auths.isEmpty) {
      return EMPTY
    }

    getAuths(auths.mkString(DELIM))
  }

  def getAuths(auths: String): AccumuloAuthorizations = {
    if (Strings.isNullOrEmpty(auths)) {
      return EMPTY
    }

    CACHE.get(auths)
  }
}
