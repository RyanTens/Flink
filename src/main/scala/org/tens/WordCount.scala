package org.tens
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
object WordCount {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    val text: DataStream[String] = env.socketTextStream("localhost", 9999, '\n')

    val windowCounts = text.flatMap(_.split("\\W"))
      .map(word => WordWithCount(word, 1))
      .keyBy("word")
      .timeWindow(Time.seconds(5))
      .sum("count")

    windowCounts.print().setParallelism(1)

    env.execute("Socket Window WordCount")

  }
  case class WordWithCount(word: String, count: Long)
}
