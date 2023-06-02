package tree.utils

import getTimeMillis
import io.netty.buffer.ByteBuf
import pt.unl.fct.di.novasys.network.ISerializer

class HybridTimestamp(val logical: Long = 0, val counter: Int = 0) {

    fun nextTimestamp(): HybridTimestamp {
        val newLogical = logical.coerceAtLeast(getTimeMillis())
        val newCounter = if (logical == newLogical) counter + 1 else 0
        return HybridTimestamp(newLogical, newCounter)
    }

    fun mergeTimestamp(other: HybridTimestamp): HybridTimestamp {
        val newLogical = getTimeMillis().coerceAtLeast(logical.coerceAtLeast(other.logical))
        return if(newLogical == logical && newLogical == other.logical)
            HybridTimestamp(newLogical, counter.coerceAtLeast(other.counter) + 1)
        else if (newLogical == logical)
            HybridTimestamp(newLogical, counter + 1)
        else if (newLogical == other.logical)
            HybridTimestamp(newLogical, other.counter + 1)
        else
            HybridTimestamp(newLogical, 0)
    }

    fun isAfter(other: HybridTimestamp): Boolean {
        return logical > other.logical || (logical == other.logical && counter > other.counter)
    }


    fun max(other: HybridTimestamp): HybridTimestamp {
        return if (isAfter(other)) this else other
    }

    fun min(other: HybridTimestamp): HybridTimestamp {
        return if (isAfter(other)) other else this
    }

    override fun toString(): String {
        return "($logical,$counter)"
    }

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false

        other as HybridTimestamp

        if (logical != other.logical) return false
        return counter == other.counter
    }

    override fun hashCode(): Int {
        var result = logical.hashCode()
        result = 31 * result + counter
        return result
    }

    object Serializer : ISerializer<HybridTimestamp> {
        override fun serialize(ts: HybridTimestamp, out: ByteBuf) {
            out.writeLong(ts.logical)
            out.writeInt(ts.counter)
        }

        override fun deserialize(buff: ByteBuf): HybridTimestamp {
            val logical = buff.readLong()
            val counter = buff.readInt()
            return HybridTimestamp(logical, counter)
        }
    }
}