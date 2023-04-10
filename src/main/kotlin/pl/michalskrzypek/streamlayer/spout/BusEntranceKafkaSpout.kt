package pl.michalskrzypek.streamlayer.spout

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.IntegerDeserializer
import org.apache.storm.kafka.spout.KafkaSpout
import org.apache.storm.kafka.spout.KafkaSpoutConfig
import org.apache.storm.kafka.spout.RecordTranslator
import org.apache.storm.tuple.Fields
import org.slf4j.LoggerFactory
import pl.michalskrzypek.streamlayer.dto.BusEntranceKafkaEvent
import pl.michalskrzypek.streamlayer.topology.SlidingWindowTopologyBuilder

/**
 * Spout that fetches and parses data from Kafka bus_entrance topic
 */
class BusEntranceKafkaSpout : KafkaSpout<Int, BusEntranceKafkaEvent>(config()) {

    companion object {
        private val LOG = LoggerFactory.getLogger(SlidingWindowTopologyBuilder::class.java)

        private const val KAFKA_HOST = "127.0.0.1"
        private const val KAFKA_PORT = "9092"
        private const val KAFKA_TOPIC = "bus_entrance"

        private val props = mapOf(
            "group.id" to "streamlayer",
            ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG to IntegerDeserializer::class.java,
            ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG to BusEntranceJsonDeserializer::class.java,
        )

        fun config(): KafkaSpoutConfig<Int, BusEntranceKafkaEvent> =
            KafkaSpoutConfig.Builder<Int, BusEntranceKafkaEvent>(
                "${KAFKA_HOST}:${KAFKA_PORT}", KAFKA_TOPIC
            )
                .setProp(props)
                .setRecordTranslator(object : RecordTranslator<Int, BusEntranceKafkaEvent> {
                    override fun apply(record: ConsumerRecord<Int, BusEntranceKafkaEvent>): MutableList<Any> {
                        LOG.info("Kafka spout record - $record")
                        return mutableListOf(
                            record.value().busId,
                            record.value().passId,
                            record.value().timestamp,
                            record.value().type
                        )
                    }

                    override fun getFieldsFor(stream: String?): Fields {
                        LOG.info("Kafka spout stream - $stream")
                        return Fields("bus_id", "pass_id", "timestamp", "type")
                    }
                })
                .build()
    }
}