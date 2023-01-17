package hyparflood.utils.messaging

import io.netty.buffer.ByteBuf
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage
import pt.unl.fct.di.novasys.network.ISerializer


class JoinReplyMessage : ProtoMessage(MSG_CODE) {
    override fun toString(): String {
        return "JoinReplyMessage{}"
    }

    companion object {
        const val MSG_CODE: Short = 306
        val serializer = object : ISerializer<JoinReplyMessage> {
            override fun serialize(m: JoinReplyMessage?, out: ByteBuf) {}

            override fun deserialize(buff: ByteBuf): JoinReplyMessage {
                return JoinReplyMessage()
            }
        }
    }
}