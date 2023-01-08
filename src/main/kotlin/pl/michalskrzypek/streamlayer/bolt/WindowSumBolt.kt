package pl.michalskrzypek.streamlayer.bolt

import com.google.gson.Gson
import com.google.gson.GsonBuilder
import org.apache.storm.task.OutputCollector
import org.apache.storm.task.TopologyContext
import org.apache.storm.topology.OutputFieldsDeclarer
import org.apache.storm.topology.base.BaseWindowedBolt
import org.apache.storm.tuple.Fields
import org.apache.storm.tuple.Tuple
import org.apache.storm.tuple.Values
import org.apache.storm.windowing.TupleWindow
import org.slf4j.LoggerFactory
import pl.michalskrzypek.streamlayer.dto.BusEntranceData
import java.time.Instant
import java.time.LocalDateTime
import java.util.*


/**
 * Computes sliding window sum.
 */
class WindowSumBolt : BaseWindowedBolt() {
    private var sum = 0
    private var collector: OutputCollector? = null
    private var gson: Gson? = null

    companion object {
        private val LOG = LoggerFactory.getLogger(WindowSumBolt::class.java)
    }

    override fun prepare(topoConf: Map<String, Any>, context: TopologyContext, collector: OutputCollector) {
        this.collector = collector
        this.gson = GsonBuilder().create()
    }

    override fun execute(inputWindow: TupleWindow) {
        LOG.info("Executing window in thread: ${Thread.currentThread().name}")

        val startTimestamp = inputWindow.startTimestamp


        /*
         * The inputWindow gives a view of
         * (a) all the events in the window
         * (b) events that expired since last activation of the window
         * (c) events that newly arrived since last activation of the window
         */
        val newTuples = inputWindow.new
        LOG.info("New tuples: $newTuples")
        val expiredTuples = inputWindow.expired
        LOG.info("Expired tuples: $expiredTuples")

        val tuplesInWindow = inputWindow.get()
        LOG.info("Tuples in the window: $tuplesInWindow")
        LOG.info("Events in current window: " + tuplesInWindow.size)
        /*
         * Instead of iterating over all the tuples in the window to compute
         * the sum, the values for the new events are added and old events are
         * subtracted. Similar optimizations might be possible in other
         * windowing computations.
         */
        for (tuple in tuplesInWindow) {
            val busEntranceData = convertTupleToBusEntranceData(tuple)
            LOG.info("JSON busEntranceData Value: $busEntranceData")
            sum += getIntByEntranceType(busEntranceData?.type.orEmpty())
        }
        LOG.info("Start: ${toLocalDateTime(inputWindow.startTimestamp)} - End: ${toLocalDateTime(inputWindow.endTimestamp)} - Sum: $sum")
        LOG.info("Bus crowdness ${(sum / 500f) * 100}%")
        collector!!.emit(Values(sum))
    }

    private fun convertTupleToBusEntranceData(tuple: Tuple): BusEntranceData? {
        val value = tuple.getStringByField("value")
        return gson!!.fromJson(value, BusEntranceData::class.java)
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