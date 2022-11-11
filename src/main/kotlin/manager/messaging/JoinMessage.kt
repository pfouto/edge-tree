package manager.messaging

import io.netty.buffer.ByteBuf
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage
import pt.unl.fct.di.novasys.network.ISerializer

class JoinMessage : ProtoMessage(MSG_CODE) {
    override fun toString(): String {
        return "JoinMessage{}"
    }

    companion object {
        const val MSG_CODE: Short = 105
        val serializer = object : ISerializer<JoinMessage> {
            override fun serialize(m: JoinMessage, out: ByteBuf) {}

            override fun deserialize(buff: ByteBuf): JoinMessage {
                return JoinMessage()
            }
        }
    }
}