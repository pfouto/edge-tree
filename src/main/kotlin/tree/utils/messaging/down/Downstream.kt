package tree.utils.messaging.down

import io.netty.buffer.ByteBuf
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage
import pt.unl.fct.di.novasys.network.ISerializer
import tree.utils.HybridTimestamp


data class Downstream(val timestamps: List<HybridTimestamp>) : ProtoMessage(ID) {

    companion object {
        const val ID: Short = 201
    }

    override fun toString(): String {
        return "Downstream(timestamps=$timestamps)"
    }

    object Serializer : ISerializer<Downstream> {
        override fun serialize(msg: Downstream, out: ByteBuf) {
            out.writeInt(msg.timestamps.size)
            for (ts in msg.timestamps) {
                HybridTimestamp.Serializer.serialize(ts, out)
            }
        }

        override fun deserialize(buff: ByteBuf): Downstream {
            val nTimestamps = buff.readInt()
            val timestamps = mutableListOf<HybridTimestamp>()
            for (i in 0 until nTimestamps) {
                val ts = HybridTimestamp.Serializer.deserialize(buff)
                timestamps.add(ts)
            }
            return Downstream(timestamps)
        }
    }

}