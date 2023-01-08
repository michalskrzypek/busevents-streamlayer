package pl.michalskrzypek.streamlayer.spout

import org.apache.kafka.clients.consumer.Consumer
import org.apache.storm.kafka.spout.internal.ConsumerFactory

class KafkaConsumerFactory: ConsumerFactory<String, String> {
    override fun createConsumer(consumerProps: MutableMap<String, Any>?): Consumer<String, String> {
        TODO("Not yet implemented")
    }
}