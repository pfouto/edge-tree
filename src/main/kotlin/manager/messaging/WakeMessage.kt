package manager.messaging

import io.netty.buffer.ByteBuf
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage
import pt.unl.fct.di.novasys.network.ISerializer
import java.net.Inet4Address

data class WakeMessage(val contact: Inet4Address) : ProtoMessage(ID) {

    companion object {
        const val ID: Short = 101

        val serializer = object : ISerializer<WakeMessage> {
            override fun serialize(msg: WakeMessage, out: ByteBuf) {
                out.writeBytes(msg.contact.address)
            }

            override fun deserialize(buff: ByteBuf): WakeMessage {
                val address = ByteArray(4)
                buff.readBytes(address)
                return WakeMessage(Inet4Address.getByAddress(address) as Inet4Address)
            }
        }
    }
}