package org.apache.spark.streaming.kafka

import scala.reflect.ClassTag
import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import kafka.serializer.Decoder
import org.apache.spark.streaming.StreamingContext

class ConfigurableDirectKafkaInputDStream[
  K: ClassTag,
  V: ClassTag,
  U <: Decoder[K]: ClassTag,
  T <: Decoder[V]: ClassTag,
  R: ClassTag](
    @transient ssc_ : StreamingContext,
    kafkaParams: Map[String, String],
    fromOffsets: Map[TopicAndPartition, Long],
    val config: Map[String, String],
    messageHandler: MessageAndMetadata[K, V] => R
) extends DirectKafkaInputDStream[K, V, U, T, R](ssc_, kafkaParams, fromOffsets, messageHandler) {
  protected override val maxMessagesPerPartition: Option[Long] = {
    val ratePerSec = if (config.keySet.contains("maxRatePerPartition"))
      config("maxRatePerPartition").toInt
    else
      context.sparkContext.getConf.getInt("spark.streaming.kafka.maxRatePerPartition", 0)

    if (ratePerSec > 0) {
      val secsPerBatch = context.graph.batchDuration.milliseconds.toDouble / 1000
      Some((secsPerBatch * ratePerSec).toLong)
    } else {
      None
    }
  }
}
