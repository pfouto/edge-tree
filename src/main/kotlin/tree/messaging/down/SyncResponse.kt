package tree.messaging.down

import io.netty.buffer.ByteBuf
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage
import pt.unl.fct.di.novasys.network.ISerializer
import storage.FetchedObject

class SyncResponse(val reconfiguration: Reconfiguration, val items: List<FetchedObject>) : ProtoMessage(ID) {

    companion object {
        const val ID: Short = 204
    }

    override fun toString(): String {
        return "SyncResponse($reconfiguration, items=${items.size})"
    }

    object Serializer : ISerializer<SyncResponse> {
        override fun serialize(msg: SyncResponse, out: ByteBuf) {
            Reconfiguration.Serializer.serialize(msg.reconfiguration, out)
            out.writeInt(msg.items.size)
            msg.items.forEach {
                FetchedObject.serialize(it, out)
            }
        }

        override fun deserialize(buff: ByteBuf): SyncResponse {
            val reconfiguration = Reconfiguration.Serializer.deserialize(buff)
            val size = buff.readInt()
            val items = mutableListOf<FetchedObject>()
            for (i in 0 until size) {
                items.add(FetchedObject.deserialize(buff))
            }
            return SyncResponse(reconfiguration, items)
        }
    }

}