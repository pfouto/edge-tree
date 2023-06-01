package tree.messaging.down

import io.netty.buffer.ByteBuf
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage
import pt.unl.fct.di.novasys.network.ISerializer
import tree.utils.HybridTimestamp


data class DownstreamMetadata(val timestamps: List<HybridTimestamp>, val persistence: Map<Int, Int>) :
    ProtoMessage(ID) {

    companion object {
        const val ID: Short = 201
    }

    override fun toString(): String {
        return "DownstreamMetadata(timestamps=$timestamps)"
    }

    object Serializer : ISerializer<DownstreamMetadata> {
        override fun serialize(msg: DownstreamMetadata, out: ByteBuf) {
            out.writeInt(msg.timestamps.size)
            for (ts in msg.timestamps) {
                HybridTimestamp.Serializer.serialize(ts, out)
            }
            out.writeInt(msg.persistence.size)
            msg.persistence.forEach { (k, v) ->
                out.writeInt(k)
                out.writeInt(v)
            }
        }

        override fun deserialize(buff: ByteBuf): DownstreamMetadata {
            val nTimestamps = buff.readInt()
            val timestamps = mutableListOf<HybridTimestamp>()
            for (i in 0 until nTimestamps) {
                val ts = HybridTimestamp.Serializer.deserialize(buff)
                timestamps.add(ts)
            }
            val nPersistence = buff.readInt()
            val persistence = mutableMapOf<Int, Int>()
            for (i in 0 until nPersistence) {
                val level = buff.readInt()
                val opId = buff.readInt()
                persistence[level] = opId
            }
            return DownstreamMetadata(timestamps, persistence)
        }
    }

}