package engage.messaging

import engage.Clock
import io.netty.buffer.ByteBuf
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage
import pt.unl.fct.di.novasys.network.ISerializer

class ResponseMessage(val id: Long, val clock: Clock?, val data: ByteArray?) : ProtoMessage(ID) {

    companion object {
        const val ID: Short = 402
    }

    object Serializer : ISerializer<ResponseMessage> {
        override fun serialize(msg: ResponseMessage, out: ByteBuf) {
            out.writeLong(msg.id)
            if (msg.clock == null)
                out.writeBoolean(false)
            else {
                out.writeBoolean(true)
                Clock.serializer.serialize(msg.clock, out)
            }
            if (msg.data != null) {
                out.writeInt(msg.data.size)
                out.writeBytes(msg.data)
            } else {
                out.writeInt(0)
            }
        }

        override fun deserialize(buff: ByteBuf): ResponseMessage {
            val id = buff.readLong()

            val hasHlc = buff.readBoolean()
            val clock: Clock? = if (hasHlc)
                Clock.serializer.deserialize(buff)
            else null

            val dataSize = buff.readInt()
            val data: ByteArray?
            if (dataSize > 0) {
                data = ByteArray(dataSize)
                buff.readBytes(data)
            } else data = null

            return ResponseMessage(id, clock, data)
        }
    }


}