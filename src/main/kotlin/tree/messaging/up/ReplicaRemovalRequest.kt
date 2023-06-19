package tree.messaging.up

import decodeUTF8
import encodeUTF8
import io.netty.buffer.ByteBuf
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage
import pt.unl.fct.di.novasys.network.ISerializer
import storage.ObjectIdentifier

data class ReplicaRemovalRequest(val deletedObjects: Set<ObjectIdentifier>, val deletedPartitions: Set<String>) :
    ProtoMessage(ID) {

    companion object {
        const val ID: Short = 213
    }

    object Serializer : ISerializer<ReplicaRemovalRequest> {
        override fun serialize(msg: ReplicaRemovalRequest, out: ByteBuf) {
            ObjectIdentifier.serializeSet(msg.deletedObjects, out)
            out.writeInt(msg.deletedPartitions.size)
            msg.deletedPartitions.forEach { encodeUTF8(it, out) }
        }

        override fun deserialize(buff: ByteBuf): ReplicaRemovalRequest {
            val objects = ObjectIdentifier.deserializeSet(buff)
            val partitions = mutableSetOf<String>()
            val size = buff.readInt()
            for (i in 0 until size)
                partitions.add(decodeUTF8(buff))
            return ReplicaRemovalRequest(objects, partitions)
        }
    }
}