package tree.utils

import getTimeMillis
import io.netty.buffer.ByteBuf
import pt.unl.fct.di.novasys.network.ISerializer

class HybridTimestamp(val logical: Long = 0, val counter: Int = 0) {

    fun nextTs(): HybridTimestamp {
        val newLogical = logical.coerceAtLeast(getTimeMillis())
        val newCounter = if (logical == newLogical) counter + 1 else 0
        return HybridTimestamp(newLogical, newCounter)
    }

    fun updatedTs(): HybridTimestamp {
        val newLogical = logical.coerceAtLeast(getTimeMillis())
        val newCounter = if (logical == newLogical) counter else 0
        return HybridTimestamp(newLogical, newCounter)
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