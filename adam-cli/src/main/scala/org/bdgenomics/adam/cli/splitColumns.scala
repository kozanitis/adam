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
import org.apache.spark.rdd.MetricsContext._
import org.bdgenomics.adam.rdd.ADAMContext._
import org.bdgenomics.formats.avro.AlignmentRecord
import parquet.hadoop.metadata.CompressionCodecName
import org.bdgenomics.adam.util.{ ParquetLogger, HadoopUtil }
import parquet.hadoop.ParquetOutputFormat
import parquet.avro.AvroParquetOutputFormat
import parquet.hadoop.util.ContextUtil
import org.apache.avro.reflect.ReflectData

object SplitColumns extends ADAMCommandCompanion {
  val commandName = "splitcolumns"
  val commandDescription = "Split into Columns"

  def apply(cmdLine: Array[String]) = {
    new SplitColumns(Args4j[SplitColumnsArgs](cmdLine))
  }
}

class SplitColumnsArgs extends Args4jBase with ParquetArgs {
  @Argument(required = true, metaVar = "ADAM",
    usage = "The ADAM Variant file", index = 0)
  var adamFile: String = _

  @Argument(required = true, metaVar = "OutputFile",
    usage = "The output file", index = 1)
  var outputFile: String = null
}

class SplitColumns(val args: SplitColumnsArgs) extends ADAMSparkCommand[SplitColumnsArgs] with Serializable {
  val companion = SplitColumns

  def run(sc: SparkContext, job: Job) {
    val reads: RDD[AlignmentRecord] = sc.loadAlignments(args.adamFile)
    val smallObject = reads.map(x => SubsetOfAdam(x.getReadName, x.getContig.getContigName,
      x.getStart, x.getEnd, x.getCigar, x.getMapq, x.getFirstOfPair))
    parquetSave(smallObject, args.outputFile)
  }

  def parquetSave[T: Manifest](rdd: RDD[T],
                               filePath: String,
                               blockSize: Int = 128 * 1024 * 1024,
                               pageSize: Int = 1 * 1024 * 1024,
                               compressCodec: CompressionCodecName = CompressionCodecName.GZIP,
                               disableDictionaryEncoding: Boolean = false) {
    //log.info("Saving data in ADAM format")

    /*if (!filePath.endsWith(".adam")) {
      log.error("Filename does not end with *.adam extension. File will be saved, but must be renamed to be opened.")
    }*/

    val job = HadoopUtil.newJob(rdd.context)
    //ParquetLogger.hadoopLoggerLevel(Level.SEVERE)
    ParquetOutputFormat.setCompression(job, compressCodec)
    ParquetOutputFormat.setEnableDictionary(job, !disableDictionaryEncoding)
    ParquetOutputFormat.setBlockSize(job, blockSize)
    ParquetOutputFormat.setPageSize(job, pageSize)
    //AvroParquetOutputFormat.setSchema(job, manifest[T].runtimeClass.asInstanceOf[Class[T]].newInstance().getSchema)
    val schema = ReflectData.get().getSchema(manifest[T].getClass())
    AvroParquetOutputFormat.setSchema(job, schema)
    // Add the Void Key
    val recordToSave = rdd.map(p => (null, p))
    // Save the values to the ADAM/Parquet file
    recordToSave.saveAsNewAPIHadoopFile(filePath,
      classOf[java.lang.Void], manifest[T].runtimeClass.asInstanceOf[Class[T]], classOf[AvroParquetOutputFormat],
      ContextUtil.getConfiguration(job))
  }
}

case class SubsetOfAdam(readName: String,
                        chromosome: String,
                        start: Long,
                        end: Long,
                        cigar: String,
                        mapq: Int,
                        firstOfPair: Boolean) extends Serializable
