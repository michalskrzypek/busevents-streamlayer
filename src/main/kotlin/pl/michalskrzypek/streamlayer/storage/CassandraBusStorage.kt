package pl.michalskrzypek.streamlayer.storage

import java.time.Instant
import java.time.LocalDateTime
import java.util.*
import kotlin.random.Random

class CassandraBusStorage {
    companion object {
        val busPassCountMap = mapOf<Int, Map<Long, Int>>()
        fun getMockPassengersCount(windowSum: Int) = Random.nextInt(0, )
    }

    fun getLastPassengersCount(busId: Int): Int = Random.nextInt(0, getBusCapacity(busId) + 1)
    fun getBusCapacity(busId: Int): Int = 500

    private fun localDateTimeFromLong(timestamp: Long): LocalDateTime =
        LocalDateTime.ofInstant(Instant.ofEpochMilli(timestamp), TimeZone.getDefault().toZoneId())
}