package proxy.messaging

import io.netty.buffer.ByteBuf
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage
import pt.unl.fct.di.novasys.network.ISerializer
import pt.unl.fct.di.novasys.network.data.Host

class ReconfigurationMessage(val hosts: List<Host>) : ProtoMessage(ID) {

    companion object {
        const val ID: Short = 404
    }

    object Serializer : ISerializer<ReconfigurationMessage> {
        override fun serialize(msg: ReconfigurationMessage, out: ByteBuf) {
            out.writeInt(msg.hosts.size)
            msg.hosts.forEach { Host.serializer.serialize(it, out) }
        }

        override fun deserialize(buff: ByteBuf): ReconfigurationMessage {
            val size = buff.readInt()
            val hosts = mutableListOf<Host>()
            repeat(size) {
                hosts.add(Host.serializer.deserialize(buff))
            }
            return ReconfigurationMessage(hosts)
        }
    }



}