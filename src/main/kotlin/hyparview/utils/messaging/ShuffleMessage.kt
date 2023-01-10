package hyparview.utils.messaging

import io.netty.buffer.ByteBuf
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage
import pt.unl.fct.di.novasys.network.ISerializer
import pt.unl.fct.di.novasys.network.data.Host


class ShuffleMessage(self: Host, peers: Collection<Host>, var ttl: Short, val seqnum: Short) :
    ProtoMessage(MSG_CODE) {

    private val sample: List<Host>
    private val origin: Host

    init {
        origin = self
        sample = ArrayList(peers)
    }

    fun getFullSample(): List<Host> {
        val full: MutableList<Host> = ArrayList(sample)
        full.add(origin)
        return full
    }

    override fun toString(): String {
        return "ShuffleMessage{" +
                "origin=" + origin +
                ", seqN=" + seqnum +
                ", ttl=" + ttl +
                ", sample=" + sample +
                '}'
    }

    fun getOrigin(): Host {
        return origin
    }

    fun decrementTtl(): Short {
        return ttl--
    }

    companion object {
        const val MSG_CODE: Short = 307
        val serializer = object : ISerializer<ShuffleMessage> {
            override fun serialize(msg: ShuffleMessage, out: ByteBuf) {
                Host.serializer.serialize(msg.origin, out)
                out.writeShort(msg.seqnum.toInt())
                out.writeShort(msg.ttl.toInt())
                out.writeShort(msg.sample.size)
                for (h in msg.sample) {
                    Host.serializer.serialize(h, out)
                }
            }

            override fun deserialize(buff: ByteBuf): ShuffleMessage {
                val origin: Host = Host.serializer.deserialize(buff)
                val seqnum = buff.readShort()
                val ttl = buff.readShort()
                val size = buff.readShort()
                val payload: MutableList<Host> = ArrayList(size.toInt())
                for (i in 0 until size) {
                    payload.add(Host.serializer.deserialize(buff))
                }
                return ShuffleMessage(origin, payload, ttl, seqnum)
            }
        }
    }
}