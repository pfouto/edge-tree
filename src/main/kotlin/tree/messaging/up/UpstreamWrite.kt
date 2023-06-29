package tree.messaging.up

import io.netty.buffer.ByteBuf
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage
import pt.unl.fct.di.novasys.network.ISerializer
import storage.RemoteWrite
import tree.utils.WriteID

data class UpstreamWrite(val writes: List<Pair<WriteID, RemoteWrite>>) : ProtoMessage(ID) {

    constructor(writeID: WriteID, singleWrite: RemoteWrite) : this(listOf(Pair(writeID, singleWrite)))

    companion object {
        const val ID: Short = 216
    }

    object Serializer : ISerializer<UpstreamWrite> {
        override fun serialize(obj: UpstreamWrite, buffer: ByteBuf) {
            buffer.writeInt(obj.writes.size)
            for (op in obj.writes) {
                buffer.writeInt(op.first.ip)
                buffer.writeInt(op.first.counter)
                buffer.writeInt(op.first.persistenceId)
                RemoteWrite.serialize(op.second, buffer)
            }
        }

        override fun deserialize(buffer: ByteBuf): UpstreamWrite {
            val nWrites = buffer.readInt()
            val writes = mutableListOf<Pair<WriteID, RemoteWrite>>()
            for (i in 0 until nWrites) {
                val ip = buffer.readInt()
                val counter = buffer.readInt()
                val persistence = buffer.readInt()
                val rw = RemoteWrite.deserialize(buffer)
                writes.add(Pair(WriteID(ip, counter, persistence), rw))
            }
            return UpstreamWrite(writes)
        }
    }
}