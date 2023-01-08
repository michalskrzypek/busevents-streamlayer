package pl.michalskrzypek.streamlayer.dto

data class BusEntranceData(
    val timestamp: String,
    val pass_id: Int,
    val bus_id: Int,
    val type: String
)