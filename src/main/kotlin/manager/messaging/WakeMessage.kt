package manager.messaging

import io.netty.buffer.ByteBuf
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage
import pt.unl.fct.di.novasys.network.ISerializer
import pt.unl.fct.di.novasys.network.data.Host

data class WakeMessage(val contact: Host?) : ProtoMessage(ID) {

    companion object {
        const val ID: Short = 109

        val serializer = object : ISerializer<WakeMessage> {
            override fun serialize(msg: WakeMessage, out: ByteBuf) {
                out.writeBoolean(msg.contact!=null)
                if (msg.contact != null)
                    Host.serializer.serialize(msg.contact, out)
            }

            override fun deserialize(buff: ByteBuf): WakeMessage {
                val hasContact = buff.readBoolean()
                val contact = if (hasContact) Host.serializer.deserialize(buff) else null
                return WakeMessage(contact)
            }
        }
    }
}