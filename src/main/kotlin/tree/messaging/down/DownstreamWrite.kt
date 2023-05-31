package tree.messaging.down

import io.netty.buffer.ByteBuf
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage
import pt.unl.fct.di.novasys.network.ISerializer
import storage.RemoteWrite


data class DownstreamWrite(val writes: List<RemoteWrite>) : ProtoMessage(ID) {

    constructor(singleWrite: RemoteWrite): this(listOf(singleWrite))

    companion object {
        const val ID: Short = 201
    }

    object Serializer : ISerializer<DownstreamWrite> {
        override fun serialize(msg: DownstreamWrite, out: ByteBuf) {
            out.writeInt(msg.writes.size)
            for (ts in msg.writes) {
                RemoteWrite.serialize(ts, out)
            }
        }

        override fun deserialize(buff: ByteBuf): DownstreamWrite {
            val nWrites = buff.readInt()
            val writes = mutableListOf<RemoteWrite>()
            for (i in 0 until nWrites) {
                val ts = RemoteWrite.deserialize(buff)
                writes.add(ts)
            }
            return DownstreamWrite(writes)
        }
    }

}