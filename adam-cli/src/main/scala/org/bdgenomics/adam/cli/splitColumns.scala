/*
 * Copyright (c) 2015. Regents of the University of California
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.bdgenomics.adam.cli

import org.kohsuke.args4j.Argument
import org.apache.spark.SparkContext
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.rdd.RDD
import org.bdgenomics.adam.rdd.ADAMContext._
import org.bdgenomics.formats.avro.{ Pileup, AlignmentRecord }
import org.bdgenomics.adam.core._

object SplitColumns extends ADAMCommandCompanion {
  val commandName = "splitcolumns"
  val commandDescription = "Split into Columns"

  def apply(cmdLine: Array[String]) = {
    new SplitColumns(Args4j[SplitColumnsArgs](cmdLine))
  }
}

class SplitColumnsArgs extends Args4jBase with ParquetArgs with Serializable {
  @Argument(required = true, metaVar = "ADAM",
    usage = "The ADAM Variant file", index = 0)
  var adamFile: String = _

}

class SplitColumns(val args: SplitColumnsArgs) extends ADAMSparkCommand[SplitColumnsArgs] with Serializable {
  val companion = SplitColumns

  def run(sc: SparkContext, job: Job) {
    val CoordinateSubsetFile = args.adamFile + ".sub"
    val PileupSubsetFile = args.adamFile + ".psub"
    val reads: RDD[AlignmentRecord] = sc.loadAlignments(args.adamFile).filter(_.getReadMapped)
    val pileups = reads.adamRecords2Pileup()
    val smallObject = reads.map(getCoordinateSubset(_))
    smallObject.adamParquetSave(CoordinateSubsetFile)
    val smallPileups = pileups.map(getPileupSubset(_))
    smallPileups.adamParquetSave(PileupSubsetFile)
  }

  def getCoordinateSubset(x: AlignmentRecord) = {
    val builder = CoordinateSubset.newBuilder()
    builder.setReadName(x.getReadName)
    builder.setChromosome(x.getContig.getContigName)
    builder.setStart(x.getStart)
    builder.setEnd(x.getEnd)
    builder.setCigar(x.getCigar)
    builder.setMapq(x.getMapq)
    builder.setFirstOfPair(x.getFirstOfPair)
    builder.build()
  }

  def getPileupSubset(x: Pileup) = {
    val builder = PileupSubset.newBuilder()
    if (x.getContig.getContigName != null)
      builder.setChromosome(x.getContig.getContigName)
    builder.setPosition(x.getPosition)
    builder.setMapq(x.getMapQuality)
    if (x.getReadBase != null)
      builder.setReadBase(x.getReadBase.toString)
    builder.setIsReverseStrand(x.getIsReverseStrand)
    builder.build()
  }
}

