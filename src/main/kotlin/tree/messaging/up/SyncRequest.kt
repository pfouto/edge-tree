package tree.messaging.up

import decodeUTF8
import encodeUTF8
import io.netty.buffer.ByteBuf
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage
import pt.unl.fct.di.novasys.network.ISerializer
import storage.ObjectMetadata

data class SyncRequest(
    val upstream: Upstream,
    val fullPartitions: MutableMap<String, Map<String, ObjectMetadata>>,
    val partialPartitions: MutableMap<String, Map<String, ObjectMetadata>>
) : ProtoMessage(ID) {

    companion object {
        const val ID: Short = 205
    }

    override fun toString(): String {
        return "SyncRequest($upstream, full=${fullPartitions.keys}, partial=${partialPartitions.keys})"
    }

    object Serializer : ISerializer<SyncRequest> {
        override fun serialize(msg: SyncRequest, out: ByteBuf) {
            Upstream.Serializer.serialize(msg.upstream, out)
            out.writeInt(msg.fullPartitions.size)
            msg.fullPartitions.forEach { (partition, objects) ->
                encodeUTF8(partition, out)
                serializePartition(objects, out)
            }
            out.writeInt(msg.partialPartitions.size)
            msg.partialPartitions.forEach { (partition, objects) ->
                encodeUTF8(partition, out)
                serializePartition(objects, out)
            }
        }

        private fun serializePartition(partition: Map<String, ObjectMetadata>, out: ByteBuf) {
            out.writeInt(partition.size)
            partition.forEach { (key, metadata) ->
                encodeUTF8(key, out)
                ObjectMetadata.serialize(metadata, out)
            }
        }


        override fun deserialize(buff: ByteBuf): SyncRequest {
            val upstream = Upstream.Serializer.deserialize(buff)
            val fullPartitions = mutableMapOf<String, Map<String, ObjectMetadata>>()
            val fullSize = buff.readInt()
            for (i in 0 until fullSize) {
                val partition = decodeUTF8(buff)
                val objects = deserializePartition(buff)
                fullPartitions[partition] = objects
            }

            val partialPartitions = mutableMapOf<String, Map<String, ObjectMetadata>>()
            val partialSize = buff.readInt()
            for (i in 0 until partialSize) {
                val partition = decodeUTF8(buff)
                val objects = deserializePartition(buff)
                partialPartitions[partition] = objects
            }
            return SyncRequest(upstream, fullPartitions, partialPartitions)
        }

        private fun deserializePartition(buff: ByteBuf): Map<String, ObjectMetadata> {
            val objects = mutableMapOf<String, ObjectMetadata>()
            val size = buff.readInt()
            for (i in 0 until size) {
                val key = decodeUTF8(buff)
                val metadata = ObjectMetadata.deserialize(buff)
                objects[key] = metadata
            }
            return objects
        }
    }

}