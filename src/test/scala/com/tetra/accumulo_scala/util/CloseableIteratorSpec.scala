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
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class CloseableIteratorSpec extends UnitSpec {
	"CloseableIterator" should "close when fully exhausted" in {
	  val ci = new ExCloseabelIterator
	  
	  var count = 0
	  ci.foreach(count += _)
	  
	  assert(15 == count)
	  assert(ci.isClosed)
	}
	
	it should "close if take(1) is called and exhausted" in {
	  val ci = new ExCloseabelIterator
	  
	  var count = 0
	  ci.take(1).foreach(count += _)
	  
	  assert(1 == count)
	  assert(ci.isClosed)
	}
	
	it should "close if drop(1) is called and exhausted" in {
	  val ci = new ExCloseabelIterator
	  
	  var count = 0
	  ci.drop(1).foreach(count += _)
	  
	  assert(14 == count)
	  assert(ci.isClosed)
	}
	
it should "close if map is called and exhausted" in {
	  val ci = new ExCloseabelIterator
	  
	  var count = 0
	  ci.map(_+1) foreach(count += _)
	  
	  assert(20 == count)
	  assert(ci.isClosed)
	}
}

class ExCloseabelIterator extends CloseableIterator[Int] {
  var isClosed = false
  val iter = List(1, 2, 3, 4, 5).iterator

  def hasNext(): Boolean = {
    if(!iter.hasNext) {
      close
    }
    iter.hasNext
  }
  def next() = iter.next
  def close() {
    isClosed = true
  }
}