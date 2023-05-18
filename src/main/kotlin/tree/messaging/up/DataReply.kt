package tree.messaging.up

import io.netty.buffer.ByteBuf
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage
import pt.unl.fct.di.novasys.network.ISerializer
import storage.FetchedObject

data class DataReply(val items: List<FetchedObject>) : ProtoMessage(ID) {

    companion object {
        const val ID: Short = 208
    }

    object Serializer : ISerializer<DataReply> {
        override fun serialize(msg: DataReply, out: ByteBuf) {
            out.writeInt(msg.items.size)
            msg.items.forEach {
                FetchedObject.serialize(it, out)
            }
        }

        override fun deserialize(buff: ByteBuf): DataReply {
            val size = buff.readInt()
            val items = mutableListOf<FetchedObject>()
            for (i in 0 until size) {
                items.add(FetchedObject.deserialize(buff))
            }
            return DataReply(items)
        }
    }
}