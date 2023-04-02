package com.zjc.flow_analysis


import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state.{ListStateDescriptor, MapState, MapStateDescriptor}
import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import java.lang
import scala.collection.JavaConversions.bufferAsJavaList
import scala.collection.mutable.ListBuffer
import org.apache.flink.streaming.api.scala._

import java.sql.Timestamp
import java.text.SimpleDateFormat

case class LogEvent(ip: String, userId: String, eventTime: Long, method: String, url: String)
case class UrlViewCount(url: String, windowEnd: Long, count: Long)

/**
 * 每隔5秒，输出最近10分钟内访问量最多的前N个URL
 */
object FlowTopNPage {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    // nc -lk 7777
    val streamData = env.socketTextStream("hadoop103", 7777)

    // 将数据封装成样例类，并设置watermark，延迟时间最大1分钟
    val dataStream = streamData.map(data => {
      val dataArray = data.split(" ")
      val sf = new SimpleDateFormat("dd/MM/yyyy:HH:mm:ss")
      val timeStamp = sf.parse(dataArray(3)).getTime

      LogEvent(dataArray(0), dataArray(1), timeStamp, dataArray(5), dataArray(6))
      // 保证1，延迟时间1s
    }).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[LogEvent](Time.seconds(1)) {
      override def extractTimestamp(t: LogEvent): Long = t.eventTime
    })
    val lateOutputTag = new OutputTag[LogEvent]("late data")
    //开窗聚合
    val aggData = dataStream.filter( data => {
      val pattern = "^((?!\\.(ico|png|css|js)$).)*$".r
      (pattern findFirstIn data.url).nonEmpty
    } )
      .keyBy(_.url).timeWindow(Time.minutes(10), Time.seconds(5))
      .allowedLateness(Time.minutes(1)) // 保证2，延迟1分钟
      .sideOutputLateData(new OutputTag[LogEvent]("late data"))// 保证3，侧输出流
      .aggregate(new PageCountAgg(), new PageCountWindowResult())

    val lateDataStream = aggData.getSideOutput(lateOutputTag)

    //每个窗口的统计值排序输出
    val resultStream = aggData.keyBy("windowEnd").process(new TopNPage(3))

    dataStream.print("data")
    aggData.print("agg")
    lateDataStream.print("late")
    resultStream.print("result")
    env.execute("top N page job.")
  }
}

class PageCountAgg() extends AggregateFunction[LogEvent, Long, Long] {
  override def createAccumulator(): Long = 0L

  override def add(in: LogEvent, acc: Long): Long = acc + 1

  override def getResult(acc: Long): Long = acc

  override def merge(acc: Long, acc1: Long): Long = acc + acc1
}

/**
 * 自定义windowFunction包装成样例类输出,如果是 .keyBy("url")则第三参数是Tuple
如果是.keyBy(data=>data.url)或 .keyBy(_.url)则第三个参数是String
 */
class PageCountWindowResult() extends WindowFunction[Long, UrlViewCount, String, TimeWindow] {
  override def apply(key: String, window: TimeWindow, input: Iterable[Long], out: Collector[UrlViewCount]): Unit = {

    out.collect(UrlViewCount(key, window.getEnd, input.head))
  }
}

class TopNPage(n: Int) extends KeyedProcessFunction[Tuple, UrlViewCount, String] {
  // 定义ListState保存所以的聚合结果
  lazy val PageCountMapState: MapState[String, Long] = getRuntimeContext.getMapState(
    new MapStateDescriptor[String, Long]("PageCountMapState", classOf[String], classOf[Long]))

  override def processElement(i: UrlViewCount, context: KeyedProcessFunction[Tuple, UrlViewCount, String]#Context,
                              collector: Collector[String]): Unit = {
    PageCountMapState.put(i.url, i.count)
    context.timerService().registerEventTimeTimer(i.windowEnd + 100)
    context.timerService().registerEventTimeTimer(i.windowEnd + 60 * 1000L)
  }

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Tuple, UrlViewCount, String]#OnTimerContext, out: Collector[String]): Unit = {
    if (timestamp == ctx.timestamp() + 60 * 1000L) {
      PageCountMapState.clear()
      return
    }
    val pcListState :ListBuffer[(String, Long)] = ListBuffer()
    val iter = PageCountMapState.entries().iterator()
    while (iter.hasNext) {
      val entry = iter.next()
      pcListState += ((entry.getKey, entry.getValue))
    }

    //    PageCountListState.clear()
    val sortedPCList = pcListState.sortWith(_._2 > _._2).take(n)
    // 或
    //    val sortedPCList1 = pcListState.sortBy(_.count)(Ordering.Long.reverse).take(n)
    // 将排名输出显示
    val result = new StringBuilder()
    result.append("时间：").append(new Timestamp(timestamp - 100)).append("\n")
    //遍历输出
    for (i <- sortedPCList.indices) {
      val curPCCount = sortedPCList.get(i)
      result.append("Top").append(i + 1).append(":")
      result.append("当前url:").append(curPCCount._1)
      result.append(",当前访问量：").append(curPCCount._2)
      result.append("\n")
    }
    result.append("--------------------------------------------\n")
    //控制输出频率
    Thread.sleep(1000L)
    out.collect(result.toString())
  }
}
