package tree.messaging.up

import io.netty.buffer.ByteBuf
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage
import pt.unl.fct.di.novasys.network.ISerializer
import storage.ObjectIdentifier

data class DataRequest(val items: Set<ObjectIdentifier>) : ProtoMessage(ID) {

    companion object {
        const val ID: Short = 207
    }

    object Serializer : ISerializer<DataRequest> {
        override fun serialize(msg: DataRequest, out: ByteBuf) {
            ObjectIdentifier.serializeSet(msg.items, out)
        }

        override fun deserialize(buff: ByteBuf): DataRequest {
            val items = ObjectIdentifier.deserializeSet(buff)
            return DataRequest(items)
        }
    }
}