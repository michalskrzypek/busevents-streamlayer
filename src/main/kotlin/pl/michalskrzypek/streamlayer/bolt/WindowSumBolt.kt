package pl.michalskrzypek.streamlayer.bolt

import org.apache.storm.task.OutputCollector
import org.apache.storm.task.TopologyContext
import org.apache.storm.topology.OutputFieldsDeclarer
import org.apache.storm.topology.base.BaseWindowedBolt
import org.apache.storm.tuple.Fields
import org.apache.storm.tuple.Tuple
import org.apache.storm.tuple.Values
import org.apache.storm.windowing.TupleWindow
import org.slf4j.LoggerFactory
import pl.michalskrzypek.streamlayer.dto.BusEntranceKafkaEvent
import java.time.Instant
import java.time.LocalDateTime
import java.util.*


/**
 * Bolt that computes sliding window sum of passengers enter/exit actions.
 */
class WindowSumBolt : BaseWindowedBolt() {
    private var collector: OutputCollector? = null

    companion object {
        private val LOG = LoggerFactory.getLogger(WindowSumBolt::class.java)
    }

    override fun prepare(topoConf: Map<String, Any>, context: TopologyContext, collector: OutputCollector) {
        this.collector = collector
    }

    override fun execute(inputWindow: TupleWindow) {
        var sum = 0

        val newTuples = inputWindow.new
        LOG.info("New tuples: $newTuples")

        val busId: Int = newTuples[0]?.let { convertTupleToBusEntranceData(it) }?.busId ?: -1
        LOG.info("Executing window for bus: $busId in thread: ${Thread.currentThread().name}")

        val from: String = newTuples[0]?.let { convertTupleToBusEntranceData(it) }?.timestamp.orEmpty()
        val to: String = newTuples[newTuples.size - 1]?.let { convertTupleToBusEntranceData(it) }?.timestamp.orEmpty()
        LOG.info("Window for bus: $busId: start - $from, to: $to")

        val expiredTuples = inputWindow.expired
        LOG.info("Expired tuples: $expiredTuples")

        val tuplesInWindow = inputWindow.get()
        LOG.info("Events in the current window: $tuplesInWindow")
        LOG.info("Events count: " + tuplesInWindow.size)

        for (tuple in newTuples) {
            val busEntranceData = convertTupleToBusEntranceData(tuple)
            LOG.info("JSON busEntranceData Value: $busEntranceData")
            sum += getIntByEntranceType(busEntranceData.type)
            collector!!.ack(tuple)
        }
        LOG.info("Start: ${toLocalDateTime(inputWindow.startTimestamp)} - End: ${toLocalDateTime(inputWindow.endTimestamp)} - Sum: $sum")
        collector!!.emit(Values(busId, sum, from, to))

    }

    private fun convertTupleToBusEntranceData(tuple: Tuple): BusEntranceKafkaEvent {
        LOG.info("Tuple: $tuple")
        val busId = tuple.getIntegerByField("bus_id")
        val passId = tuple.getIntegerByField("pass_id")
        val timestamp = tuple.getStringByField("timestamp")
        val type = tuple.getStringByField("type")
        return BusEntranceKafkaEvent(timestamp, passId, busId, type)
    }

    override fun declareOutputFields(declarer: OutputFieldsDeclarer) {
        declarer.declare(Fields("bus_id", "sum", "from", "to"))
    }

    private fun getIntByEntranceType(type: String): Int {
        return when (type) {
            "IN" -> 1
            "OUT" -> -1
            else -> 0
        }
    }

    private fun toLocalDateTime(long: Long): LocalDateTime = LocalDateTime.ofInstant(
        Instant.ofEpochMilli(long),
        TimeZone.getDefault().toZoneId()
    )
}