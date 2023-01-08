package pl.michalskrzypek.streamlayer.bolt

import org.apache.storm.task.OutputCollector
import org.apache.storm.task.TopologyContext
import org.apache.storm.topology.OutputFieldsDeclarer
import org.apache.storm.topology.base.BaseRichBolt
import org.apache.storm.tuple.Fields
import org.apache.storm.tuple.Tuple
import org.apache.storm.tuple.Values
import org.slf4j.LoggerFactory
import pl.michalskrzypek.streamlayer.topology.SlidingWindowTopologyBuilder

/**
 * Gets a minute sum of passenger bus entrance, fetches last bus passenger count,
 * calculates new sum, sends the result to Cassandra,
 * fetches
 *
 */
class DataPersistenceBolt : BaseRichBolt() {

    companion object {
        private val LOG = LoggerFactory.getLogger(SlidingWindowTopologyBuilder::class.java)
    }

    private var collector: OutputCollector? = null

    override fun prepare(topoConf: Map<String, Any>, context: TopologyContext, collector: OutputCollector) {
        this.collector = collector
    }

    override fun execute(input: Tuple) {

        val windowSum = input.getIntegerByField("sum")
        collector!!.emit(Values(sum / tuplesInWindow.size))
    }

    override fun declareOutputFields(declarer: OutputFieldsDeclarer) {
        declarer.declare(Fields("total"))
    }
}