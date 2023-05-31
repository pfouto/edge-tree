package tree.messaging.up

import io.netty.buffer.ByteBuf
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage
import pt.unl.fct.di.novasys.network.ISerializer
import storage.RemoteWrite
import tree.utils.HybridTimestamp

data class UpstreamWrite(val writes: List<RemoteWrite>) : ProtoMessage(ID) {

    constructor(singleWrite: RemoteWrite) : this(listOf(singleWrite))

    companion object {
        const val ID: Short = 215
    }

    object Serializer : ISerializer<UpstreamWrite> {
        override fun serialize(obj: UpstreamWrite, buffer: ByteBuf) {
            buffer.writeInt(obj.writes.size)
            for (ts in obj.writes) {
                RemoteWrite.serialize(ts, buffer)
            }
        }

        override fun deserialize(buffer: ByteBuf): UpstreamWrite {
            val nWrites = buffer.readInt()
            val writes = mutableListOf<RemoteWrite>()
            for (i in 0 until nWrites) {
                val ts = RemoteWrite.deserialize(buffer)
                writes.add(ts)
            }
            return UpstreamWrite(writes)
        }
    }
}