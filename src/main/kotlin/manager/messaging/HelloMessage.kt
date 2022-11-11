package manager.messaging

import io.netty.buffer.ByteBuf
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage
import pt.unl.fct.di.novasys.network.ISerializer

//prio == true -> high; prio == false --> low
class HelloMessage(val isPriority: Boolean) : ProtoMessage(MSG_CODE) {

    override fun toString(): String {
        return "HelloMessage{" +
                "priority=" + isPriority +
                '}'
    }

    companion object {
        const val MSG_CODE: Short = 103

        val serializer = object : ISerializer<HelloMessage> {
            override fun serialize(m: HelloMessage, out: ByteBuf) {
                out.writeBoolean(m.isPriority)
            }

            override fun deserialize(buff: ByteBuf): HelloMessage {
                val priority = buff.readBoolean()
                return HelloMessage(priority)
            }
        }
    }
}