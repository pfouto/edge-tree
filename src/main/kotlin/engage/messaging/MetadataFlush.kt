package engage.messaging

import io.netty.buffer.ByteBuf
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage
import pt.unl.fct.di.novasys.network.ISerializer
import java.net.InetAddress

class MetadataFlush(val updates: MutableMap<InetAddress, Int>) : ProtoMessage(MSG_ID) {

    companion object {
        const val MSG_ID: Short = 203

        fun single(addr: InetAddress, value: Int): MetadataFlush {
            val updates = mutableMapOf<InetAddress, Int>()
            updates[addr] = value
            return MetadataFlush(updates)
        }

        val serializer = object : ISerializer<MetadataFlush> {

            override fun serialize(msg: MetadataFlush, out: ByteBuf) {
                out.writeInt(msg.updates.size)
                msg.updates.forEach { (k, v) ->
                    out.writeBytes(k.address)
                    out.writeInt(v)
                }
            }

            override fun deserialize(input: ByteBuf): MetadataFlush {
                val mapSize = input.readInt()
                val updates = mutableMapOf<InetAddress, Int>()
                for (i in 0 until mapSize) {
                    val addrBytes = ByteArray(4)
                    input.readBytes(addrBytes)
                    val vUp = input.readInt()
                    updates[InetAddress.getByAddress(addrBytes)] = vUp
                }
                return MetadataFlush(updates)
            }
        }
    }

    override fun toString(): String {
        return "MetadataFlush(updates=$updates)"
    }

    fun merge(source: InetAddress, vUp: Int) {
        updates.merge(source, vUp, Math::max)
    }

    fun merge(other: MetadataFlush) {
        other.updates.forEach { (k, v) -> updates.merge(k, v, Math::max) }
    }


}