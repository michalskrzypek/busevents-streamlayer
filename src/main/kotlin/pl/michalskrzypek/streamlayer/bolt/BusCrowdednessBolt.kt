package pl.michalskrzypek.streamlayer.bolt

import com.datastax.driver.core.ResultSet
import com.datastax.driver.core.Row
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.Producer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.IntegerSerializer
import org.apache.storm.task.OutputCollector
import org.apache.storm.task.TopologyContext
import org.apache.storm.topology.OutputFieldsDeclarer
import org.apache.storm.topology.base.BaseRichBolt
import org.apache.storm.tuple.Fields
import org.apache.storm.tuple.Tuple
import org.slf4j.LoggerFactory
import pl.michalskrzypek.streamlayer.dto.BusCrowdednessKafkaEvent
import pl.michalskrzypek.streamlayer.storage.CassandraStorage
import java.util.*


/**
 * Bolt that calculates and sends the current sum of passengers in a bus to Cassandra
 * and calculates crowdedness of a bus and sends results to a Kafka topic.
 */
class BusCrowdednessBolt : BaseRichBolt() {

    companion object {
        private val LOG = LoggerFactory.getLogger(BusCrowdednessBolt::class.java)
    }

    private var collector: OutputCollector? = null
    private var cassandraStorage: CassandraStorage? = null
    private var kafkaProducer: Producer<Int, BusCrowdednessKafkaEvent>? = null

    override fun prepare(topoConf: Map<String, Any>, context: TopologyContext, collector: OutputCollector) {
        this.collector = collector
        this.cassandraStorage = CassandraStorage()
        this.kafkaProducer = createProducer("http://localhost:9092")
    }

    override fun cleanup() {
        this.cassandraStorage?.close()
    }

    override fun execute(input: Tuple) {
        val busId = input.getIntegerByField("bus_id")
        val sum = input.getIntegerByField("sum")
        val from = input.getStringByField("from")
        val to = input.getStringByField("to")

        val lastPassCount = selectLastPassCount(busId, from)
        val passCount = lastPassCount?.getInt(0) ?: 1
        LOG.info("Pass count: $passCount")
        val capacity = lastPassCount?.getInt(1) ?: 1
        LOG.info("Capacity: $capacity")
        val newPassCount = passCount.plus(sum)
        LOG.info("New pass count: $newPassCount")
        val crowdedness = newPassCount.div(capacity.toDouble())
        LOG.info("Crowdedness: $crowdedness")
        insertNewPassCount(busId, to, capacity, newPassCount)
        sendBusCrowdednessToKafka(busId, crowdedness, to)
        collector!!.ack(input)
    }

    private fun sendBusCrowdednessToKafka(busId: Int, crowdedness: Double, to: String) {
        kafkaProducer?.send(
            ProducerRecord(
                "bus_crowdedness",
                busId,
                BusCrowdednessKafkaEvent(
                    busId,
                    crowdedness,
                    to
                )
            )
        )
    }

    private fun createProducer(brokers: String): Producer<Int, BusCrowdednessKafkaEvent> {
        val props = Properties()
        props["bootstrap.servers"] = brokers
        props["key.serializer"] = IntegerSerializer::class.java
        props["value.serializer"] = org.springframework.kafka.support.serializer.JsonSerializer::class.java
        return KafkaProducer(props)
    }

    private fun selectLastPassCount(busId: Int, timestamp: String): Row? {
        return cassandraStorage
            ?.session
            ?.execute(
                "SELECT pass_count, capacity FROM bus_pass_count WHERE bus_id = $busId AND timestamp < '$timestamp' ORDER BY timestamp DESC LIMIT 1;"
            )
            ?.first()
    }

    private fun insertNewPassCount(busId: Int, timestamp: String, capacity: Int, passCount: Int): ResultSet? {
        return cassandraStorage
            ?.session
            ?.execute(
                "INSERT INTO bus_pass_count (bus_id , timestamp , capacity , pass_count ) VALUES ($busId, '$timestamp', $capacity, $passCount)"
            )
    }

    override fun declareOutputFields(declarer: OutputFieldsDeclarer) {
        declarer.declare(Fields("total"))
    }
}