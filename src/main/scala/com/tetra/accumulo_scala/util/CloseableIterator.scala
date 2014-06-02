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

/**
 * An iterator that needs to be closed
 * 
 * Implementers must call 'close' when 'hasNext' returns 'false'
 */
trait CloseableIterator[+T] extends Iterator[T] with java.io.Closeable { self =>

  /**
   * Implementers must call 'close' when 'hasNext' returns 'false'
   * 
   * {@inheritDoc}
   */
  override def close(): Unit

  /**
   * {@inheritDoc}
   */
  override def map[B](f: T => B): CloseableIterator[B] = new CloseableIterator[B] {
    def hasNext = self.hasNext
    def next() = f(self.next())
    def close() = self.close()
  }

  /**
   * need to override to support drop, take and slice
   * 
   * {@inheritDoc}
   */
  override def slice(from: Int, until: Int): CloseableIterator[T] = {
    var skip = from max 0
    while (skip > 0 && self.hasNext) {
      skip -= 1
      self.next
    }

    new CloseableIterator[T] {
      var remaining = until
      
      def hasNext: Boolean = {
        if (remaining > 0 && self.hasNext) true
        else {
          close
          false
        }
      }
      
      def next(): T = {
        remaining -= 1
        self.next()
      }
      
      def close() = self.close()
    }
  }
}

object CloseableIterator {
  def empty() = new CloseableIterator[Nothing] {
    override def hasNext = false
    override def next() = throw new NoSuchElementException
    override def close() = {}
  }

  implicit def iterator2CloseableIterator[T](iter: Iterator[T]) = new CloseableIterator[T] {
    def hasNext = iter.hasNext
    def next() = iter.next()
    def close() = { /*noop*/ }
  }
}