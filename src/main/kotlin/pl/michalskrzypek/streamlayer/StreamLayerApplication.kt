package pl.michalskrzypek.streamlayer

import org.apache.storm.Config
import org.apache.storm.LocalCluster

import org.apache.storm.StormSubmitter
import pl.michalskrzypek.streamlayer.topology.SlidingWindowTopologyBuilder
import pl.michalskrzypek.streamlayer.topology.SlidingWindowTopologyBuilder.ONE_MINUTE_DURATION_IN_MS

fun main(args: Array<String>) {
    val conf = Config()
    conf.setDebug(true)
    conf.setNumWorkers(1)
    conf.setMessageTimeoutSecs(ONE_MINUTE_DURATION_IN_MS + 1)

    val slidingWindowTopology = SlidingWindowTopologyBuilder.build()
    var topoName = "sliding-window-topology"

    val cluster = LocalCluster()
    cluster.submitTopology(topoName, conf, slidingWindowTopology)
    Thread.sleep(100000)
}
