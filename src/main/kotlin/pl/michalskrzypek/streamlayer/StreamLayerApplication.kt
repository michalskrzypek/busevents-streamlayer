package pl.michalskrzypek.streamlayer

import com.datastax.driver.core.Cluster
import com.datastax.driver.core.Session
import com.datastax.driver.core.exceptions.NoHostAvailableException
import com.datastax.driver.core.policies.DowngradingConsistencyRetryPolicy
import com.datastax.driver.core.policies.ExponentialReconnectionPolicy
import com.datastax.driver.core.policies.RoundRobinPolicy
import com.datastax.driver.core.policies.TokenAwarePolicy
import org.apache.storm.Config
import org.apache.storm.LocalCluster

import org.apache.storm.StormSubmitter
import pl.michalskrzypek.streamlayer.bolt.BusCrowdednessBolt
import pl.michalskrzypek.streamlayer.topology.SlidingWindowTopologyBuilder
import pl.michalskrzypek.streamlayer.topology.SlidingWindowTopologyBuilder.ONE_MINUTE_DURATION_IN_MS
import java.util.concurrent.TimeUnit

fun main(args: Array<String>) {
    val conf = Config()
    conf.setDebug(true)
    conf.setNumWorkers(1)
    conf.setMessageTimeoutSecs(ONE_MINUTE_DURATION_IN_MS + 1)

    val slidingWindowTopology = SlidingWindowTopologyBuilder.build()
    val topoName = "sliding-window-topology"

    val cluster = LocalCluster()
    cluster.submitTopology(topoName, conf, slidingWindowTopology)
    Thread.sleep(100000)
}
