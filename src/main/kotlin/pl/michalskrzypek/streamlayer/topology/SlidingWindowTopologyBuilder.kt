package pl.michalskrzypek.streamlayer.topology

import org.apache.storm.generated.StormTopology
import org.apache.storm.topology.TopologyBuilder
import org.apache.storm.topology.base.BaseWindowedBolt
import org.apache.storm.tuple.Fields
import pl.michalskrzypek.streamlayer.bolt.BusCrowdednessBolt
import pl.michalskrzypek.streamlayer.bolt.WindowSumBolt
import pl.michalskrzypek.streamlayer.spout.BusEntranceKafkaSpout


object SlidingWindowTopologyBuilder {

    const val ONE_MINUTE_DURATION_IN_MS = 1000 * 60

    fun build(): StormTopology {
        val builder = TopologyBuilder()
        val kafkaSpout = BusEntranceKafkaSpout()
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
            3
        )
            .fieldsGrouping("kafka_spout", Fields("bus_id"))

        builder.setBolt(
            "bus_crowdedness_bolt",
            BusCrowdednessBolt(),
            3
        )
            .fieldsGrouping("passengers_window_sum_bolt", Fields("bus_id"))
        return builder.createTopology()
    }
}