package proxy.messaging

import io.netty.buffer.ByteBuf
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage
import pt.unl.fct.di.novasys.network.ISerializer

class PersistenceMessage(val id: Long) : ProtoMessage(ID) {

    companion object {
        const val ID: Short = 403
    }

    object Serializer : ISerializer<PersistenceMessage> {
        override fun serialize(msg: PersistenceMessage, out: ByteBuf) {
            out.writeLong(msg.id)
        }

        override fun deserialize(buff: ByteBuf): PersistenceMessage {
            val id = buff.readLong()
            return PersistenceMessage(id)
        }
    }



}