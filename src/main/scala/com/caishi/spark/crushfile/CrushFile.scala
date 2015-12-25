package com.caishi.spark.crushfile

import org.apache.hadoop.fs._
import org.apache.spark.sql.{SaveMode, SQLContext}
import org.apache.spark.{SparkContext, SparkConf}

/**
 * 合并日志文件：目前支持parquet file
 * 1.从fromDir目录中加载数据文件
 * 2.将文件输出到tmpDir，文件已arh字符开头
 * 3.mv tmpDir中的文件到fromDir中
 * 4.删除fromDir目录中非arh开头的文件
 * Created by root on 15-10-28.
 */
object CrushFile {
  def main(args: Array[String]) {
    if (args.length < 3) {
      System.err.println("Usage: " +
        "| CrushFile <dfsUri> <abFromDir> <abTmpDir>")
      System.exit(1)
    }
    val Array(dfsUri,fromDir,tmpDir) = args
//    val dfsUri = "hdfs://10.4.1.4:9000"
//    val fromDir ="/test/dw/2015/10/28/topic_common_event/13"
//    val tmpDir = "/test/tmp"


    val sparkConf = new SparkConf()
//    sparkConf.setAppName("spark-crushfile").setMaster("local")
    sparkConf.setAppName("spark-crushfile")
    val sc = new SparkContext(sparkConf)
    sc.hadoopConfiguration.set("fs.defaultFS",dfsUri)
    val fs = FileSystem.get(sc.hadoopConfiguration)
    val sql = new SQLContext(sc)
    sql.setConf("spark.sql.parquet.compression.codec", "snappy")

    val df = sql.read.parquet(fromDir)
    df.repartition(takePartition(fromDir,fs)).write.format("parquet").mode(SaveMode.Overwrite).save(tmpDir)
    // 删除源目录 mv 新文件
    mv(tmpDir,fromDir,fs)
    del(fromDir,fs)
  }

  /** 根据输入目录计算目录大小，并以128*2M大小计算partition */
  def takePartition(src : String,fs : FileSystem): Int ={
    val cos:ContentSummary = fs.getContentSummary(new Path(src))
    val sizeM:Long = cos.getLength/1024/1024
    val partNum:Int = sizeM/256 match {
      case 0 => 1
      case _ => (sizeM/256).toInt
    }
    partNum
  }

  /** 将临时目录中的结果文件mv到源目录，并以ahr-为文件前缀 */
  def mv (fromDir:String,toDir:String,fs:FileSystem): Unit ={
    val srcFiles : Array[Path] = FileUtil.stat2Paths(fs.listStatus(new Path(fromDir)));
    for(p : Path <- srcFiles){
      // 如果是以part开头的文件则修改名称
      if(p.getName.startsWith("part")){
        fs.rename(p,new Path(toDir+"/ahr-"+p.getName))
      }
    }
  }

  /** 删除原始小文件 */
  def del(fromDir : String,fs : FileSystem): Unit ={
    val files : Array[Path] = FileUtil.stat2Paths(fs.listStatus(new Path(fromDir),new FileFilter()))
    for(f : Path <- files){
      fs.delete(f,true)// 迭代删除文件或目录
    }
  }
}

class FileFilter extends  PathFilter{
  @Override  def accept(path : Path) : Boolean = {
    !path.getName().startsWith("ahr-")
  }
}
