package tree.messaging.down

import encodeUTF8
import decodeUTF8
import io.netty.buffer.ByteBuf
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage
import pt.unl.fct.di.novasys.network.ISerializer
import storage.ObjectData

data class PartitionReplicationReply(val partition: String, val objects: List<Pair<String, ObjectData>>) :
    ProtoMessage(ID) {

    companion object {
        const val ID: Short = 210
    }

    object Serializer : ISerializer<PartitionReplicationReply> {
        override fun serialize(msg: PartitionReplicationReply, out: ByteBuf) {
            encodeUTF8(msg.partition, out)
            out.writeInt(msg.objects.size)
            msg.objects.forEach {
                encodeUTF8(it.first, out)
                ObjectData.serialize(it.second, out)
            }
        }

        override fun deserialize(buff: ByteBuf): PartitionReplicationReply {
            val partition = decodeUTF8(buff)
            val size = buff.readInt()
            val objects = mutableListOf<Pair<String, ObjectData>>()
            for (i in 0 until size) {
                val key = decodeUTF8(buff)
                val data = ObjectData.deserialize(buff)
                objects.add(Pair(key, data))
            }
            return PartitionReplicationReply(partition, objects)
        }
    }
}