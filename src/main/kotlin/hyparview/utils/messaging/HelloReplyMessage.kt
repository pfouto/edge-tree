package hyparview.utils.messaging

import io.netty.buffer.ByteBuf
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage
import pt.unl.fct.di.novasys.network.ISerializer


class HelloReplyMessage(val isTrue: Boolean) : ProtoMessage(MSG_CODE) {

    override fun toString(): String {
        return "HelloReplyMessage{" +
                "reply=" + isTrue +
                '}'
    }

    companion object {
        const val MSG_CODE: Short = 304
        val serializer = object : ISerializer<HelloReplyMessage> {
            override fun serialize(m: HelloReplyMessage, out: ByteBuf) {
                out.writeBoolean(m.isTrue)
            }

            override fun deserialize(buff: ByteBuf): HelloReplyMessage {
                val reply = buff.readBoolean()
                return HelloReplyMessage(reply)
            }
        }
    }
}