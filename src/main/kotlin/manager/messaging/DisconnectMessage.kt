package manager.messaging

import io.netty.buffer.ByteBuf
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage
import pt.unl.fct.di.novasys.network.ISerializer


class DisconnectMessage : ProtoMessage(MSG_CODE) {

    override fun toString(): String {
        return "DisconnectMessage{}"
    }

    companion object {
        const val MSG_CODE: Short = 101

        val serializer = object : ISerializer<DisconnectMessage> {
            override fun serialize(m: DisconnectMessage?, out: ByteBuf) {}

            override fun deserialize(buff: ByteBuf): DisconnectMessage {
                return DisconnectMessage()
            }
        }
    }
}