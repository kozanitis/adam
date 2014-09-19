/**
 * Licensed to Big Data Genomics (BDG) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The BDG licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.bdgenomics.adam.rdd.RegionMultijoin

import org.bdgenomics.adam.models.ReferenceRegion
import scala.annotation.tailrec

class IntervalTree[T](chr: String, allRegions: List[(ReferenceRegion, T)]) extends Serializable {
  val chromosome = chr

  /*For now let's require that allRegions should come from the same chromosome. For multi-chromosome support
  it is easy to modify the class as follows: The "root" instead of a single node becomes a hash table that
  is indexed by a chromosome name and its contents point to different interval trees. In that case, method
  getAllOverlappings will have to search that hash table to get to the root of the tree that corresponds
  to the query chromosome before calling allOverlappingRegions.
   */
  assert(allSameChromosome(allRegions))
  val root = new Node(allRegions)

  @tailrec private def allSameChromosome(regions: List[(ReferenceRegion, T)]): Boolean = regions match {
    case Nil => true
    case h :: tail if (!h._1.referenceName.equals(chromosome)) => false
    case h :: tail => allSameChromosome(tail)
  }

  def getAllOverlappings(r: ReferenceRegion) = allOverlappingRegions(r, root)

  private def allOverlappingRegions(r: ReferenceRegion, rt: Node): List[(ReferenceRegion, T)] = {
    if (!r.referenceName.equals(chromosome))
      return Nil
    if (rt == null)
      return Nil
    val resultFromThisNode = r match {
      case x if (rt.inclusiveIntervals == Nil) => Nil //Sometimes a node can have zero intervals
      case x if (x.end < rt.minPointOfCollection || x.start > rt.maxPointOfCollection) => Nil //Save unnecessary filtering
      case _ => rt.inclusiveIntervals.filter(t => r.overlaps(t._1))
    }

    if (r.overlaps(ReferenceRegion(chromosome, rt.centerPoint, rt.centerPoint + 1)))
      return resultFromThisNode ++ allOverlappingRegions(r, rt.leftChild) ++ allOverlappingRegions(r, rt.rightChild)
    else if (r.end <= rt.centerPoint)
      return resultFromThisNode ++ allOverlappingRegions(r, rt.leftChild)
    else if (r.start > rt.centerPoint)
      return resultFromThisNode ++ allOverlappingRegions(r, rt.rightChild)
    else throw new NoSuchElementException("Interval Tree Exception. Illegal comparison for centerpoint " + rt.centerPoint)

  }

  class Node(allRegions: List[(ReferenceRegion, T)]) {

    private val largestPoint = allRegions.maxBy(_._1.end)._1.end
    private val smallestPoint = allRegions.minBy(_._1.start)._1.start
    val centerPoint = smallestPoint + (largestPoint - smallestPoint) / 2

    val (inclusiveIntervals, leftChild, rightChild) = distributeRegions()
    val minPointOfCollection: Long = inclusiveIntervals match {
      case Nil => -1
      case _   => inclusiveIntervals.minBy(_._1.start)._1.start
    }

    val maxPointOfCollection: Long = inclusiveIntervals match {
      case Nil => -1
      case _   => inclusiveIntervals.maxBy(_._1.end)._1.end
    }

    def distributeRegions() = {
      var leftRegions: List[(ReferenceRegion, T)] = Nil
      var rightRegions: List[(ReferenceRegion, T)] = Nil
      var centerRegions: List[(ReferenceRegion, T)] = Nil

      allRegions.foreach(x => {
        if (x._1.end < centerPoint) leftRegions ::= x
        else if (x._1.start > centerPoint) rightRegions ::= x
        else centerRegions ::= x
      })

      val leftChild: Node = leftRegions match {
        case Nil => null
        case _   => new Node(leftRegions)
      }

      val rightChild: Node = rightRegions match {
        case Nil => null
        case _   => new Node(rightRegions)
      }
      (centerRegions, leftChild, rightChild)
    }

  }

}
