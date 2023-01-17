package hyparflood.utils.messaging

import io.netty.buffer.ByteBuf
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage
import pt.unl.fct.di.novasys.network.ISerializer
import pt.unl.fct.di.novasys.network.data.Host

class ForwardJoinMessage(var ttl: Short, val newHost: Host) : ProtoMessage(MSG_CODE) {

    override fun toString(): String {
        return "ForwardJoinMessage{" +
                "ttl=" + ttl +
                ", newHost=" + newHost +
                '}'
    }

    fun decrementTtl(): Short {
        return ttl-- //decrement after returning
    }

    companion object {
        const val MSG_CODE: Short = 302
        val serializer: ISerializer<ForwardJoinMessage> = object : ISerializer<ForwardJoinMessage> {
            override fun serialize(m: ForwardJoinMessage, out: ByteBuf) {
                out.writeShort(m.ttl.toInt())
                Host.serializer.serialize(m.newHost, out)
            }

            override fun deserialize(buff: ByteBuf): ForwardJoinMessage {
                val ttl = buff.readShort()
                val newHost: Host = Host.serializer.deserialize(buff)
                return ForwardJoinMessage(ttl, newHost)
            }
        }
    }
}