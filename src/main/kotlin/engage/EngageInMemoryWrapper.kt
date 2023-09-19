package engage

import io.netty.buffer.ByteBuf
import org.apache.logging.log4j.LogManager
import decodeUTF8
import encodeUTF8

class EngageInMemoryWrapper {

    companion object {
        private val logger = LogManager.getLogger()
    }

    private val data = mutableMapOf<String, MutableMap<String, ObjectData>>()

    fun initialize() {
        logger.info("In memory storage initialized")
    }

    fun put(objId: ObjectIdentifier, objData: ObjectData): ObjectData {
        val partition = data.computeIfAbsent(objId.partition) { mutableMapOf() }
        return partition.merge(objId.key, objData)
        { old, new -> if (new.metadata.isAfter(old.metadata)) new else old }!!
    }

    fun get(objId: ObjectIdentifier): ObjectData? {
        return data[objId.partition]?.get(objId.key)
    }

    fun getMetadata(objId: ObjectIdentifier): ObjectMetadata? {
        return data[objId.partition]?.get(objId.key)?.metadata
    }

    fun cleanUp() {
        data.forEach { (_, v) -> v.clear() }
        data.clear()
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

data class ObjectMetadata(val vc: Clock, val lww: Int) {
    companion object {
        fun serialize(obj: ObjectMetadata, out: ByteBuf) {
            Clock.serializer.serialize(obj.vc, out)
            out.writeInt(obj.lww)
        }

        fun deserialize(buff: ByteBuf): ObjectMetadata {
            val vc = Clock.serializer.deserialize(buff)
            val lww = buff.readInt()
            return ObjectMetadata(vc, lww)
        }
    }

    fun isAfter(other: ObjectMetadata): Boolean {
        return lww > other.lww
    }

}

data class ObjectData(val value: ByteArray, val metadata: ObjectMetadata) {

    override fun toString(): String {
        return "ObjectData(size=${value.size}, metadata=$metadata)"
    }
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


