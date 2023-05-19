package tree.messaging.up

import io.netty.buffer.ByteBuf
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage
import pt.unl.fct.di.novasys.network.ISerializer
import storage.DataIndex
import storage.ObjectIdentifier

data class SyncRequest(
    val upstream: Upstream,
    val objects: DataIndex,
) : ProtoMessage(ID) {

    companion object {
        const val ID: Short = 205
    }

    override fun toString(): String {
        return "SyncRequest($upstream, objects=$objects)"
    }

    object Serializer : ISerializer<SyncRequest> {
        override fun serialize(msg: SyncRequest, out: ByteBuf) {
            Upstream.Serializer.serialize(msg.upstream, out)
            ObjectIdentifier.serializeSet(msg.objects, out)
        }

        override fun deserialize(buff: ByteBuf): SyncRequest {
            val upstream = Upstream.Serializer.deserialize(buff)
            val items = ObjectIdentifier.deserializeSet(buff)
            return SyncRequest(upstream, items)
        }
    }

}