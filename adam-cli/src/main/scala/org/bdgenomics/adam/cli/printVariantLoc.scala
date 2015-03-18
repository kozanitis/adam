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
import org.bdgenomics.formats.avro.Genotype
import org.apache.spark.SparkContext.rddToPairRDDFunctions
import org.bdgenomics.adam.rdd.ADAMContext._

object PrintVariantLoc extends ADAMCommandCompanion {
  val commandName = "printvariantloc"
  val commandDescription = "Print variant loci"

  def apply(cmdLine: Array[String]) = {
    new PrintVariantLoc(Args4j[PrintVariantLocArgs](cmdLine))
  }
}

class PrintVariantLocArgs extends Args4jBase with ParquetArgs {
  @Argument(required = true, metaVar = "ADAM",
    usage = "The ADAM Variant file", index = 0)
  var adamFile: String = _
}

class PrintVariantLoc(val args: PrintVariantLocArgs) extends ADAMSparkCommand[PrintVariantLocArgs] with Serializable {
  val companion = PrintVariantLoc
  def run(sc: SparkContext, job: Job) {
    val adamVariants: RDD[Genotype] = sc.loadGenotypes(args.adamFile)
    val printInfo = adamVariants.map(x => ((x.getVariant.getContig.getContigName,
      x.getVariant.getStart, x.getVariant.getEnd), 1))
    val keys = printInfo.reduceByKey(_ + _).map(_._1).map(x => "" + x._1 + " " + x._2 + " " + x._3)
    keys.collect.foreach(println)

  }
}
