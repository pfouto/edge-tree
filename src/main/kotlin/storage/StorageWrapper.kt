package storage

import decodeUTF8
import encodeUTF8
import io.netty.buffer.ByteBuf
import tree.utils.HybridTimestamp

interface StorageWrapper {
    fun initialize()
    fun put(objId: ObjectIdentifier, objData: ObjectData): ObjectData
    fun get(objId: ObjectIdentifier): ObjectData?
    fun delete(objId: ObjectIdentifier): ObjectData?
    fun cleanUp()
    fun getFullPartitionData(partition: String): List<Pair<String, ObjectData>>
}

data class RemoteWrite(val objectIdentifier: ObjectIdentifier, val objectData: ObjectData)

data class FetchedObject(val objectIdentifier: ObjectIdentifier, val objectData: ObjectData?){
    companion object{
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

data class ObjectIdentifier(val partition: String, val key: String){
    companion object{
        fun serializeSet(set: Set<ObjectIdentifier>, out: ByteBuf) {
            out.writeInt(set.size)
            set.forEach {
                encodeUTF8(it.partition,out)
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

data class ObjectData(val value: ByteArray, val hlc: HybridTimestamp, val lww: Int) {

    companion object {
        fun serialize(obj: ObjectData, out: ByteBuf) {
            out.writeInt(obj.value.size)
            out.writeBytes(obj.value)
            HybridTimestamp.Serializer.serialize(obj.hlc, out)
            out.writeInt(obj.lww)
        }

        fun deserialize(buff: ByteBuf): ObjectData {
            val value = ByteArray(buff.readInt())
            buff.readBytes(value)
            val hlc = HybridTimestamp.Serializer.deserialize(buff)
            val lww = buff.readInt()
            return ObjectData(value, hlc, lww)
        }
    }

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false

        other as ObjectData

        if (!value.contentEquals(other.value)) return false
        if (hlc != other.hlc) return false
        return lww == other.lww
    }

    override fun hashCode(): Int {
        var result = value.contentHashCode()
        result = 31 * result + hlc.hashCode()
        result = 31 * result + lww
        return result
    }
}
