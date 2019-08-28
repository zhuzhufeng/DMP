package utils

import org.apache.spark.graphx.{Edge, Graph, VertexId, VertexRDD}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 图计算实例
  */
object graph_test {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[*]")
    val sc = new SparkContext(conf)
    //构造点的集合
    val vertexRDD = sc.makeRDD(Seq(
      (1L, ("詹姆斯", 35)),
      (2L, ("霍华德", 34)),
      (6L, ("杜兰特", 31)),
      (9L, ("库里", 30)),
      (133L, ("哈登", 30)),
      (138L, ("席尔瓦", 36)),
      (16L, ("法尔考", 35)),
      (44L, ("内马尔", 27)),
      (21L, ("J罗", 28)),
      (5L, ("高斯林", 60)),
      (7L, ("奥德斯基", 55)),
      (158L, ("码云", 55))
    ))
    //构造边得集合-->同一终端的点才能互相连接成边
    /**
      *  Edge(1L, 133L, 0)三个属性
      *  第一个参数和第二个参数:初始点和结束点
      *  第三个参数:属性值
     */
    val egde: RDD[Edge[Int]] = sc.makeRDD(Seq(
      Edge(1L, 133L, 0),
      Edge(2L, 133L, 0),
      Edge(6L, 133L, 0),
      Edge(9L, 133L, 0),
      Edge(6L, 138L, 0),
      Edge(16L, 138L, 0),
      Edge(44L, 138L, 0),
      Edge(21L, 138L, 0),
      Edge(5L, 158L, 0),
      Edge(7L, 158L, 0)
    ))

    /**
      * 构建图
      * 取出数值最小的顶点(也称为最大顶点)
      */
    val graph = Graph(vertexRDD,egde)
    //取出每个边上的最大顶点-->取出数值最小的顶点(也称为最大顶点) -->已关联到顶点
   val vertices: VertexRDD[VertexId] = graph.connectedComponents().vertices
    vertices.foreach(println)
    vertices.join(vertexRDD).map{
      case (userId,(conId,(name,age)))=>{
        (conId,List(name,age))
      }
    }.reduceByKey(_++_).foreach(println)




  }

}
