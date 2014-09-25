/*
 * Copyright (c) 2014. Regents of the University of California
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
import org.bdgenomics.adam.rdd.ADAMContext._
import org.apache.spark.rdd.RDD
import org.bdgenomics.formats.avro.AlignmentRecord
import org.bdgenomics.adam.models.ReferenceRegion
import org.bdgenomics.adam.rdd.RegionMultijoin.YetAnotherRegionJoin
import org.bdgenomics.adam.rich.ReferenceMappingContext._

object JoinBed extends ADAMCommandCompanion {
  val commandName = "joinbed"
  val commandDescription = "Region Join between ADAM and BED files"

  def apply(cmdLine: Array[String]) = {
    new JoinBed(Args4j[JoinBedArgs](cmdLine))
  }
}

class JoinBedArgs extends Args4jBase with ParquetArgs {
  @Argument(required = true, metaVar = "ADAM", usage = "The ADAM Variant file", index = 0)
  var adamFile: String = ""

  @Argument(required = true, metaVar = "BED", usage = "The Bed file", index = 1)
  var bedFile: String = null

  @Argument(required = true, metaVar = "Output", usage = "The output file", index = 2)
  var output: String = null
}

class JoinBed(protected val args: JoinBedArgs) extends ADAMSparkCommand[JoinBedArgs] {
  val companion: ADAMCommandCompanion = JoinBed

  def run(sc: SparkContext, job: Job): Unit = {

    val adamRDD: RDD[AlignmentRecord] = sc.adamLoad(args.adamFile)
    val mappedRDD = adamRDD.filter(_.getReadMapped)
      .map(x => (new ReferenceRegion(x.getContig.getContigName.toString, x.getStart, x.getEnd)))

    val bedRDD = sc.textFile(args.bedFile).map(x => {
      val splitX = x.split("\\s+")
      new ReferenceRegion(splitX(0), splitX(1).asInstanceOf[Long], splitX(2).asInstanceOf[Long])
    })

    YetAnotherRegionJoin.overlapJoin(sc, bedRDD, mappedRDD).saveAsTextFile(args.output)

  }
}
