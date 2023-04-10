package pl.michalskrzypek.streamlayer.storage

import com.datastax.driver.core.Cluster
import com.datastax.driver.core.Session
import com.datastax.driver.core.exceptions.NoHostAvailableException
import com.datastax.driver.core.policies.DowngradingConsistencyRetryPolicy
import com.datastax.driver.core.policies.ExponentialReconnectionPolicy
import com.datastax.driver.core.policies.RoundRobinPolicy
import com.datastax.driver.core.policies.TokenAwarePolicy
import org.slf4j.LoggerFactory
import java.util.concurrent.TimeUnit

class CassandraStorage {
    private var cluster: Cluster? = null
    var session: Session? = null

    companion object {
        private val LOG = LoggerFactory.getLogger(CassandraStorage::class.java)
    }

    init {
        val cassandraClient = setupCassandraClient("localhost")
        this.cluster = cassandraClient
        this.session = getSessionWithRetry(cassandraClient, "streamlayer")
    }

    private fun getSessionWithRetry(cluster: Cluster, keyspace: String?): Session? {
        while (true) {
            try {
                return cluster.connect(keyspace)
            } catch (e: NoHostAvailableException) {
                LOG.warn("All Cassandra Hosts offline. Waiting to try again.")
                Thread.sleep(1000)
            }
        }
    }

    private fun setupCassandraClient(nodes: String): Cluster {
        return Cluster
            .builder()
            .addContactPoints(nodes)
            .withPort(9042)
            .withRetryPolicy(DowngradingConsistencyRetryPolicy.INSTANCE)
            .withReconnectionPolicy(ExponentialReconnectionPolicy(100L, TimeUnit.MINUTES.toMillis(5)))
            .withLoadBalancingPolicy(TokenAwarePolicy(RoundRobinPolicy()))
            .build()
    }

    fun close() {
        session?.close()
        cluster?.close()
    }
}