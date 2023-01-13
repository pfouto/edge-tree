package hyparview.utils.messaging

import io.netty.buffer.ByteBuf
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage
import pt.unl.fct.di.novasys.network.ISerializer
import pt.unl.fct.di.novasys.network.data.Host

class BroadcastMessage(val origin: Host, val seqNumber: Int, val payload: ByteArray) : ProtoMessage(MSG_CODE) {

    override fun toString(): String {
        return "BroadcastMessage{" +
                "origin=" + origin +
                ", seqNumber=" + seqNumber +
                '}'
    }

    companion object {
        const val MSG_CODE: Short = 309

        val serializer = object : ISerializer<BroadcastMessage> {
            override fun serialize(msg: BroadcastMessage, out: ByteBuf) {
                Host.serializer.serialize(msg.origin, out)
                out.writeInt(msg.seqNumber)
                out.writeInt(msg.payload.size)
                out.writeBytes(msg.payload)
            }

            override fun deserialize(buff: ByteBuf): BroadcastMessage {
                val origin = Host.serializer.deserialize(buff)
                val seqNumber = buff.readInt()
                val size = buff.readInt()
                val payload = ByteArray(size)
                buff.readBytes(payload)
                return BroadcastMessage(origin, seqNumber, payload)
            }
        }
    }
}