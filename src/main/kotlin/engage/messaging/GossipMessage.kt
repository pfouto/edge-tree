package engage.messaging

import deserializeString
import io.netty.buffer.ByteBuf
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage
import pt.unl.fct.di.novasys.network.ISerializer
import pt.unl.fct.di.novasys.network.data.Host
import serializeString

class GossipMessage(val parts: Map<String, Pair<Host, Int>>) : ProtoMessage(MSG_ID) {

    companion object {
        const val MSG_ID: Short = 101

        val serializer = object : ISerializer<GossipMessage> {

            override fun serialize(msg: GossipMessage, out: ByteBuf) {
                out.writeInt(msg.parts.size)
                msg.parts.forEach {
                    serializeString(it.key, out)
                    Host.serializer.serialize(it.value.first, out)
                    out.writeInt(it.value.second)
                }
            }

            override fun deserialize(input: ByteBuf): GossipMessage {
                val nElements = input.readInt()
                val map: MutableMap<String, Pair<Host, Int>> = mutableMapOf()
                for (i in 0 until nElements) {
                    val key = deserializeString(input)
                    val host = Host.serializer.deserialize(input)
                    val steps = input.readInt()
                    map[key] = Pair(host, steps)
                }
                return GossipMessage(map)
            }
        }
    }

    override fun toString(): String {
        return "GossipMessage(parts=$parts)"
    }
}