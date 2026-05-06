package com.github.robran.adserver.load

import java.io.File
import kotlin.math.pow
import kotlin.random.Random

/**
 * Pre-generates a CSV feeder for Gatling simulations. Run once before each simulation; the file
 * is regenerated on every invocation so seed-driven randomness stays stable across runs.
 *
 * CSV columns: user_id, banner_w, banner_h, slot_id
 *
 * User IDs are drawn from a Zipfian distribution over a 100K pool — heavy users will hit
 * frequency caps quickly under load; the long tail keeps cardinality realistic.
 */
object WorkloadGenerator {

    private const val USER_POOL_SIZE = 100_000
    private const val ROW_COUNT = 100_000
    private const val ZIPF_S = 1.07
    private val BANNER_SIZES = listOf(300 to 250, 728 to 90, 160 to 600, 300 to 600)
    private val SLOT_IDS = (1..50).map { "slot-%03d".format(it) }

    fun generate(outFile: File, seed: Long = 42L) {
        outFile.parentFile.mkdirs()
        val rnd = Random(seed)
        val zipfCdf = buildZipfCdf(USER_POOL_SIZE, ZIPF_S)
        outFile.bufferedWriter().use { w ->
            w.write("user_id,banner_w,banner_h,slot_id")
            w.newLine()
            repeat(ROW_COUNT) {
                val rank = sampleZipfRank(rnd, zipfCdf)
                val userId = "user-${"%06d".format(rank)}"
                val (bw, bh) = BANNER_SIZES.random(rnd)
                val slotId = SLOT_IDS.random(rnd)
                w.write("$userId,$bw,$bh,$slotId")
                w.newLine()
            }
        }
    }

    private fun buildZipfCdf(n: Int, s: Double): DoubleArray {
        val weights = DoubleArray(n) { i -> 1.0 / (i + 1).toDouble().pow(s) }
        val total = weights.sum()
        val cdf = DoubleArray(n)
        var running = 0.0
        for (i in 0 until n) {
            running += weights[i] / total
            cdf[i] = running
        }
        return cdf
    }

    private fun sampleZipfRank(rnd: Random, cdf: DoubleArray): Int {
        val u = rnd.nextDouble()
        var lo = 0
        var hi = cdf.size - 1
        while (lo < hi) {
            val mid = (lo + hi) ushr 1
            if (cdf[mid] < u) lo = mid + 1 else hi = mid
        }
        return lo
    }

    @JvmStatic
    fun main(args: Array<String>) {
        val out = File(args.getOrElse(0) { "load-test/build/feeders/users.csv" })
        generate(out)
        println("Wrote ${out.length()} bytes to ${out.absolutePath}")
    }
}
