package proxy.messaging

import io.netty.buffer.ByteBuf
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage
import pt.unl.fct.di.novasys.network.ISerializer
import tree.utils.HybridTimestamp

class ResponseMessage(val id: Long, val hlc: HybridTimestamp, val data: ByteArray?) : ProtoMessage(ID) {

    companion object {
        const val ID: Short = 402
    }

    object Serializer : ISerializer<ResponseMessage> {
        override fun serialize(msg: ResponseMessage, out: ByteBuf) {
            out.writeLong(msg.id)
            HybridTimestamp.Serializer.serialize(msg.hlc, out)
            if (msg.data != null) {
                out.writeInt(msg.data.size)
                out.writeBytes(msg.data)
            } else {
                out.writeInt(0)
            }
        }

        override fun deserialize(buff: ByteBuf): ResponseMessage {
            val id = buff.readLong()
            val hlc: HybridTimestamp = HybridTimestamp.Serializer.deserialize(buff)
            val dataSize = buff.readInt()
            val data: ByteArray?
            if (dataSize > 0) {
                data = ByteArray(dataSize)
                buff.readBytes(data)
            } else {
                data = null
            }
            return ResponseMessage(id, hlc, data)
        }
    }



}