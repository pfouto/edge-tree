package tree.messaging.up

import io.netty.buffer.ByteBuf
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage
import pt.unl.fct.di.novasys.network.ISerializer
import storage.ObjectIdentifier

data class ObjectReplicationRequest(val items: Set<ObjectIdentifier>) : ProtoMessage(ID) {

    companion object {
        const val ID: Short = 211
    }

    object Serializer : ISerializer<ObjectReplicationRequest> {
        override fun serialize(msg: ObjectReplicationRequest, out: ByteBuf) {
            ObjectIdentifier.serializeSet(msg.items, out)
        }

        override fun deserialize(buff: ByteBuf): ObjectReplicationRequest {
            val items = ObjectIdentifier.deserializeSet(buff)
            return ObjectReplicationRequest(items)
        }
    }
}