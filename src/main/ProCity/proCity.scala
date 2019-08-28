import java.util.Properties

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SQLContext}

/**
  * 统计各省市
  */
object proCity {
  def main(args: Array[String]): Unit = {
    // System.setProperty("hadoop.home.dir","dir/2016-10-01_06_p1_invalid.1475274123982.log")
    // 判断路径是否正确
    if(args.length != 2){
      println("目录参数不正确，退出程序")
      sys.exit()
    }
    // 创建一个集合保存输入和输出目录
    val Array(inputPath,outputPath) = args
    val conf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[*]")
      // 设置序列化方式 采用Kyro序列化方式，比默认序列化方式性能高
      .set("spark.serializer","org.apache.spark.serializer.KryoSerializer")
    // 创建执行入口
    val sc = new SparkContext(conf)
    val sQLContext = new SQLContext(sc)
    // 设置压缩方式 使用Snappy方式进行压缩
    sQLContext.setConf("spark.sql.parquet.compression.codec","snappy")
    //读取本地文件
    val df: DataFrame = sQLContext.read.parquet(inputPath)
    //注册临时表=>视图
    df.registerTempTable("log")
    val result: DataFrame = sQLContext.sql("select provincename,cityname,count(*) from log group by provincename,cityname")
   // result.coalesce(1).write.json(outputPath)
//    //1.按省市分区存到本地    partitionBy("provincename","cityname") 第一个为一级目录
     result.write.partitionBy("provincename","cityname").json(outputPath)
//    //2.存到MySQL 记载配置文件才需要
//    val load: Config = ConfigFactory.load()
   //    val pro: Properties = new Properties()
   //
   //    pro.setProperty("user",load.getString("jdbc.user"))
  //    pro.setProperty("password",load.getString("jdbc.password"))
  //    result.write.mode("append").jdbc(load.getString("jdbc.url"),load.getString("jdbc.TableName"),pro)

    sc.stop()


  }

}
