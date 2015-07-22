package org.apache.spark.streaming.kafka

import kafka.message.MessageAndMetadata
import kafka.serializer.Decoder
import org.apache.spark.SparkException
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.InputDStream
import scala.reflect.ClassTag

object MoreKafkaUtils {
  def createDirectStream[
    K: ClassTag,
    V: ClassTag,
    KD <: Decoder[K]: ClassTag,
    VD <: Decoder[V]: ClassTag] (
    ssc: StreamingContext,
    kafkaParams: Map[String, String],
    config: Map[String, String],
    topics: Set[String]
  ): InputDStream[(K, V)] = {
    val messageHandler = (mmd: MessageAndMetadata[K, V]) => (mmd.key, mmd.message)
    val kc = new KafkaCluster(kafkaParams)
    val reset = kafkaParams.get("auto.offset.reset").map(_.toLowerCase)

    val result = for {
      topicPartitions <- kc.getPartitions(topics).right
      leaderOffsets <- (if (reset == Some("smallest")) {
        kc.getEarliestLeaderOffsets(topicPartitions)
      } else {
        kc.getLatestLeaderOffsets(topicPartitions)
      }).right
    } yield {
      val fromOffsets = leaderOffsets.map { case (tp, lo) =>
        (tp, lo.offset)
      }
      new ConfigurableDirectKafkaInputDStream[K, V, KD, VD, (K, V)](
        ssc, kafkaParams, fromOffsets, config, messageHandler)
    }
    result.fold(
      errs => throw new SparkException(errs.mkString("\n")),
      ok => ok)
  }
}
