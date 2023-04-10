package pl.michalskrzypek.streamlayer.dto

import com.google.gson.annotations.SerializedName

data class BusEntranceKafkaEvent(
    @SerializedName("timestamp") val timestamp: String,
    @SerializedName("pass_id") val passId: Int,
    @SerializedName("bus_id") val busId: Int,
    @SerializedName("type") val type: String
)