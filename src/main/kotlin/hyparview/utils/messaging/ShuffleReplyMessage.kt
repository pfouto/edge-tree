package hyparview.utils.messaging

import io.netty.buffer.ByteBuf
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage
import pt.unl.fct.di.novasys.network.ISerializer
import pt.unl.fct.di.novasys.network.data.Host


class ShuffleReplyMessage(peers: Collection<Host>, val seqnum: Short) : ProtoMessage(MSG_CODE) {

    private val sample: List<Host>

    override fun toString(): String {
        return "ShuffleReplyMessage{" +
                "seqN=" + seqnum +
                ", sample=" + sample +
                '}'
    }

    fun getSample(): List<Host> {
        return sample
    }

    init {
        sample = ArrayList(peers)
    }

    companion object {
        const val MSG_CODE: Short = 308
        val serializer = object : ISerializer<ShuffleReplyMessage> {

            override fun serialize(msg: ShuffleReplyMessage, out: ByteBuf) {
                out.writeShort(msg.seqnum.toInt())
                out.writeShort(msg.sample.size)
                for (h in msg.sample) {
                    Host.serializer.serialize(h, out)
                }
            }

            override fun deserialize(buff: ByteBuf): ShuffleReplyMessage {
                val seqnum = buff.readShort()
                val size = buff.readShort()
                val payload: MutableList<Host> = ArrayList(size.toInt())
                for (i in 0 until size) {
                    payload.add(Host.serializer.deserialize(buff))
                }
                return ShuffleReplyMessage(payload, seqnum)
            }
        }
    }
}