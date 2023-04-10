package pl.michalskrzypek.streamlayer.spout

import com.google.gson.Gson
import org.apache.kafka.common.serialization.Deserializer
import org.slf4j.LoggerFactory
import pl.michalskrzypek.streamlayer.dto.BusEntranceKafkaEvent

class BusEntranceJsonDeserializer : Deserializer<BusEntranceKafkaEvent> {

    private val gson = Gson()

    companion object {
        private val LOG = LoggerFactory.getLogger(BusEntranceJsonDeserializer::class.java)
    }

    override fun deserialize(topic: String, data: ByteArray): BusEntranceKafkaEvent? {
        return try {
            gson.fromJson(String(data), BusEntranceKafkaEvent::class.java)
        } catch (e: Throwable) {
            LOG.error("Error occurred during deserialization kafka record to BusEntranceKafkaEvent: ${e.message}")
            null
        }
    }
}