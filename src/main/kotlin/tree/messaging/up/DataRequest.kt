package tree.messaging.up

import io.netty.buffer.ByteBuf
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage
import pt.unl.fct.di.novasys.network.ISerializer

data class DataRequest(val items: Set<Pair<String, String>>) : ProtoMessage(ID) {

    companion object {
        const val ID: Short = 207
    }

    object Serializer : ISerializer<DataRequest> {
        override fun serialize(msg: DataRequest, out: ByteBuf) {
            out.writeInt(msg.items.size)
            for (item in msg.items) {
                val keyBytes = item.first.encodeToByteArray()
                out.writeInt(keyBytes.size)
                out.writeBytes(keyBytes)
                val valueBytes = item.second.encodeToByteArray()
                out.writeInt(valueBytes.size)
                out.writeBytes(valueBytes)
            }
        }

        override fun deserialize(buff: ByteBuf): DataRequest {
            val nItems = buff.readInt()
            val items = mutableSetOf<Pair<String, String>>()
            for (i in 0 until nItems) {
                val keyBytes = ByteArray(buff.readInt())
                buff.readBytes(keyBytes)
                val key = String(keyBytes)
                val valueBytes = ByteArray(buff.readInt())
                buff.readBytes(valueBytes)
                val value = String(valueBytes)
                items.add(Pair(key, value))
            }
            return DataRequest(items)
        }
    }
}