package tree.messaging.up

import io.netty.buffer.ByteBuf
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage
import pt.unl.fct.di.novasys.network.ISerializer
import tree.utils.HybridTimestamp

data class UpstreamMetadata(val ts: HybridTimestamp): ProtoMessage(ID) {
    companion object {
        const val ID: Short = 214
    }

    object Serializer : ISerializer<UpstreamMetadata> {
        override fun serialize(obj: UpstreamMetadata, buffer: ByteBuf) {
            HybridTimestamp.Serializer.serialize(obj.ts, buffer)
        }

        override fun deserialize(buffer: ByteBuf): UpstreamMetadata {
            val ts = HybridTimestamp.Serializer.deserialize(buffer)
            return UpstreamMetadata(ts)
        }
    }
}