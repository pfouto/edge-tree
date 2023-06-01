package tree.messaging.down

import io.netty.buffer.ByteBuf
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage
import pt.unl.fct.di.novasys.network.ISerializer
import storage.RemoteWrite
import tree.utils.WriteID


data class DownstreamWrite(val writes: List<Pair<WriteID, RemoteWrite>>) : ProtoMessage(ID) {

    constructor(writeID: WriteID, singleWrite: RemoteWrite) : this(listOf(Pair(writeID, singleWrite)))

    companion object {
        const val ID: Short = 202
    }

    object Serializer : ISerializer<DownstreamWrite> {
        override fun serialize(msg: DownstreamWrite, out: ByteBuf) {
            out.writeInt(msg.writes.size)
            for (write in msg.writes) {
                out.writeInt(write.first.ip)
                out.writeInt(write.first.counter)
                out.writeInt(write.first.persistence)
                RemoteWrite.serialize(write.second, out)
            }
        }

        override fun deserialize(buff: ByteBuf): DownstreamWrite {
            val nWrites = buff.readInt()
            val writes = mutableListOf<Pair<WriteID, RemoteWrite>>()
            for (i in 0 until nWrites) {
                val ip = buff.readInt()
                val counter = buff.readInt()
                val persistence = buff.readInt()
                val rw = RemoteWrite.deserialize(buff)
                writes.add(Pair(WriteID(ip, counter, persistence), rw))
            }
            return DownstreamWrite(writes)
        }
    }

}