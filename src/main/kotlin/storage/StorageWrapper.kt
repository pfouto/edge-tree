package storage

import decodeUTF8
import encodeUTF8
import io.netty.buffer.ByteBuf
import tree.utils.HybridTimestamp

interface StorageWrapper {
    fun initialize()
    fun put(objId: ObjectIdentifier, objData: ObjectData): ObjectData
    fun get(objId: ObjectIdentifier): ObjectData?
    fun getMetadata(objId: ObjectIdentifier): ObjectMetadata?
    fun delete(objId: ObjectIdentifier): ObjectData?
    fun deletePartition(partition: String)
    fun cleanUp()
    fun getPartitionDataIfNewer(partition: String, metadata: Map<String, ObjectMetadata>): List<FetchedObject>
    fun getFullPartitionData(partition: String): List<Pair<String, ObjectData>>
    fun getFullPartitionMetadata(partition: String): Map<String, ObjectMetadata>
}

data class RemoteWrite(val objectIdentifier: ObjectIdentifier, val objectData: ObjectData){
    companion object{
        fun serialize(remoteWrite: RemoteWrite, out: ByteBuf){
            encodeUTF8(remoteWrite.objectIdentifier.partition, out)
            encodeUTF8(remoteWrite.objectIdentifier.key, out)
            ObjectData.serialize(remoteWrite.objectData, out)
        }

        fun deserialize(buff: ByteBuf): RemoteWrite{
            val partition = decodeUTF8(buff)
            val key = decodeUTF8(buff)
            val objectData = ObjectData.deserialize(buff)
            return RemoteWrite(ObjectIdentifier(partition, key), objectData)
        }
    }
}

data class FetchedObject(val objectIdentifier: ObjectIdentifier, val objectData: ObjectData?) {
    companion object {
        fun serialize(fetchedObject: FetchedObject, out: ByteBuf) {
            encodeUTF8(fetchedObject.objectIdentifier.partition, out)
            encodeUTF8(fetchedObject.objectIdentifier.key, out)
            if (fetchedObject.objectData == null) {
                out.writeBoolean(false)
            } else {
                out.writeBoolean(true)
                ObjectData.serialize(fetchedObject.objectData, out)
            }
        }

        fun deserialize(buff: ByteBuf): FetchedObject {
            val partition = decodeUTF8(buff)
            val key = decodeUTF8(buff)
            val hasData = buff.readBoolean()
            val objectData = if (hasData) ObjectData.deserialize(buff) else null
            return FetchedObject(ObjectIdentifier(partition, key), objectData)
        }
    }
}

data class ObjectIdentifier(val partition: String, val key: String) {

    override fun toString(): String {
        return "$partition:$key"
    }
    companion object {
        fun serializeSet(set: Set<ObjectIdentifier>, out: ByteBuf) {
            out.writeInt(set.size)
            set.forEach {
                encodeUTF8(it.partition, out)
                encodeUTF8(it.key, out)
            }
        }

        fun deserializeSet(buff: ByteBuf): Set<ObjectIdentifier> {
            val items = mutableSetOf<ObjectIdentifier>()
            val size = buff.readInt()
            for (i in 0 until size) {
                val partition = decodeUTF8(buff)
                val key = decodeUTF8(buff)
                items.add(ObjectIdentifier(partition, key))
            }
            return items
        }

    }
}

data class ObjectMetadata(val hlc: HybridTimestamp, val lww: Int) {
    companion object {
        fun serialize(obj: ObjectMetadata, out: ByteBuf) {
            HybridTimestamp.Serializer.serialize(obj.hlc, out)
            out.writeInt(obj.lww)
        }

        fun deserialize(buff: ByteBuf): ObjectMetadata {
            val hlc = HybridTimestamp.Serializer.deserialize(buff)
            val lww = buff.readInt()
            return ObjectMetadata(hlc, lww)
        }
    }

    fun isAfter(other: ObjectMetadata): Boolean {
        return hlc.isAfter(other.hlc) || (hlc == other.hlc && lww > other.lww)
    }

}

data class ObjectData(val value: ByteArray, val metadata: ObjectMetadata) {

    companion object {
        fun serialize(obj: ObjectData, out: ByteBuf) {
            out.writeInt(obj.value.size)
            out.writeBytes(obj.value)
            ObjectMetadata.serialize(obj.metadata, out)
        }

        fun deserialize(buff: ByteBuf): ObjectData {
            val value = ByteArray(buff.readInt())
            buff.readBytes(value)
            val metadata = ObjectMetadata.deserialize(buff)
            return ObjectData(value, metadata)
        }
    }

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false

        other as ObjectData

        if (!value.contentEquals(other.value)) return false
        return metadata == other.metadata
    }

    override fun hashCode(): Int {
        var result = value.contentHashCode()
        result = 31 * result + metadata.hashCode()
        return result
    }


}
