package com.zjc.hotitems_analysis

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.common.state.ListStateDescriptor
import org.apache.flink.api.java.tuple.{Tuple, Tuple1}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala.function.WindowFunction

import java.sql.Timestamp
import scala.collection.mutable.ListBuffer
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.util.Collector

import java.lang
import java.util.Properties

// 定义输入数据的样例类
case class UserBehavior(userId: Long, itemId: Long, categoryId: Long, behavior: String, timestamp: Long)
// 定义窗口聚合结果的样例类
case class ItemViewCount(itemId: Long, windowEnd: Long, count: Long)

/**
 * 每隔5分钟输出最近一小时内点击量最多的前N个商品
 */
object HotItems {
  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "hadoop103:9092")
    properties.setProperty("group.id", "consumer-group")
    properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("auto.offset.reset", "latest")

    val inputStream = env.addSource(new FlinkKafkaConsumer[String]("hotItems", new SimpleStringSchema(), properties))


    val dataStream = inputStream.map(data => {
      val dataArray = data.split(",")
      UserBehavior(dataArray(0).toLong, dataArray(1).toLong,dataArray(2).toLong, dataArray(3).toString, dataArray(4).toLong)
    }).assignAscendingTimestamps(_.timestamp * 1000L)

    // 这个地方不能写‘pv’只能写“pv”
    val aggStream = dataStream.filter(_.behavior == "pv")
      .keyBy("itemId")
      .timeWindow(Time.hours(1), Time.minutes(5))
      // 定义窗口聚合规则(这里不是来一条就累加，而是到达滑动间隔时，将该间隔【5分钟】的数据进行累加)， 定义输出数据结构
      .aggregate(new CountAgg(), new ItemCountWindowResult())
      .keyBy("windowEnd")
      .process(new TopNHotItems(5))
    aggStream.print()

    env.execute("hot items job")

  }
}
// 定义窗口聚合规则
class CountAgg() extends AggregateFunction[UserBehavior,Long, Long] {
  override def createAccumulator(): Long = 0L

  override def add(in: UserBehavior, acc: Long): Long = acc + 1;

  override def getResult(acc: Long): Long = acc

  override def merge(acc: Long, acc1: Long): Long = acc + acc1
}
// 定义输出数据结构
class ItemCountWindowResult()extends WindowFunction[Long, ItemViewCount, Tuple, TimeWindow]{
  override def apply(key: Tuple, window: TimeWindow, input: Iterable[Long], out: Collector[ItemViewCount]): Unit = {
    val itemId = key.asInstanceOf[Tuple1[Long]].f0
    val windowEnd = window.getEnd
    val count = input.iterator.next()
    out.collect(ItemViewCount(itemId, windowEnd, count))
  }
}

class TopNHotItems(n : Int) extends KeyedProcessFunction[Tuple, ItemViewCount, String] {

  lazy val itemCountListState = getRuntimeContext.getListState(
    new ListStateDescriptor[ItemViewCount]("itemcount-list", classOf[ItemViewCount]))

  override def processElement(i: ItemViewCount, context: KeyedProcessFunction[Tuple, ItemViewCount, String]#Context,
                              collector: Collector[String]): Unit = {
    itemCountListState.add(i)
    // 注册定时器，在windowEnd + 100触发(同一个key的同一个窗口只会注册一次，即使重复注册也是相当于注册一次)
    context.timerService().registerEventTimeTimer(i.windowEnd + 100)
  }

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Tuple, ItemViewCount, String]#OnTimerContext,
                       out: Collector[String]): Unit = {
    // 将状态中的数据提取到一个listbuffer中
    val  allItemCountList:ListBuffer[ItemViewCount] = ListBuffer()
    import scala.collection.JavaConversions._
    for (itemCount <- itemCountListState.get()) {
      allItemCountList += itemCount
    }
    // 清空listState
    itemCountListState.clear()

    val sortedItemCountList = allItemCountList.sortBy(_.count)(Ordering.Long.reverse).take(n)

    // 将排名输出显示
    val result = new StringBuilder()
    result.append("时间：").append(new Timestamp(timestamp - 100)).append("\n")
    //遍历输出
    for (i <- sortedItemCountList.indices) {
      val curItemCount = sortedItemCountList.get(i)
      result.append("Top").append(i + 1).append(":")
      result.append("当前商品id:").append(curItemCount.itemId)
      result.append("当前商品访问量：").append(curItemCount.count)
      result.append("\n")
    }
    result.append("--------------------------------------------\n")
    //控制输出频率
    Thread.sleep(1000L)
    out.collect(result.toString())
  }


}

