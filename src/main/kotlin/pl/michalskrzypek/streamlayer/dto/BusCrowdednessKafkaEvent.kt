package pl.michalskrzypek.streamlayer.dto

data class BusCrowdednessKafkaEvent(
    val busId: Int,
    val crowdedness: Double,
    val timestamp: String
)