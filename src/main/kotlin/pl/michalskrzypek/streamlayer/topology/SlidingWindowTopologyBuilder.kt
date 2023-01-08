package pl.michalskrzypek.streamlayer.topology

import org.apache.storm.generated.StormTopology
import org.apache.storm.kafka.spout.KafkaSpout
import org.apache.storm.kafka.spout.KafkaSpoutConfig
import org.apache.storm.topology.TopologyBuilder
import org.apache.storm.topology.base.BaseWindowedBolt
import org.apache.storm.tuple.Fields
import org.slf4j.LoggerFactory
import pl.michalskrzypek.streamlayer.bolt.WindowSumBolt


/**
 * A sample topology that demonstrates the usage of [org.apache.storm.topology.IWindowedBolt]
 * to calculate sliding window sum.
 */
object SlidingWindowTopologyBuilder {

    private val LOG = LoggerFactory.getLogger(SlidingWindowTopologyBuilder::class.java)

    //    private val KAFKA_HOST = "192.168.100.106"
    private const val KAFKA_HOST = "127.0.0.1"
    private const val KAFKA_PORT = "9092"
    private const val KAFKA_TOPIC = "bus_entrance_2"
    const val ONE_MINUTE_DURATION_IN_MS = 1000 * 60

    fun build(): StormTopology {
        val builder = TopologyBuilder()

        val props = mapOf(
            "group.id" to "my-group"
        )

        val kafkaSpout = KafkaSpout(
            KafkaSpoutConfig.builder("$KAFKA_HOST:$KAFKA_PORT", KAFKA_TOPIC)
                .setProp(props)
                .build()
        )

        builder.setSpout(
            "kafka_spout",
            kafkaSpout,
            1
        )
        builder.setBolt(
            "passengers_window_sum_bolt",
            WindowSumBolt().withWindow(
                BaseWindowedBolt.Duration.of(ONE_MINUTE_DURATION_IN_MS),
                BaseWindowedBolt.Duration.of(ONE_MINUTE_DURATION_IN_MS)
            ),
            1
        )
            .fieldsGrouping("kafka_spout", Fields("bus_id"))
//        builder.setBolt(
//            "passengers_avg_bolt",
//            PassengersAvgBolt().withTumblingWindow(BaseWindowedBolt.Count.of(3)), 1
//        )
//            .shuffleGrouping("passengers_sum_bolt")
//        builder.setBolt("printer", PrinterBolt(), 1).shuffleGrouping("tumblingavg")
        return builder.createTopology()
    }
}