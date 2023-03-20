package tree.messaging.down

import io.netty.buffer.ByteBuf
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage
import pt.unl.fct.di.novasys.network.ISerializer

data class SyncResponse(val reconfiguration: Reconfiguration, val data: ByteArray) : ProtoMessage(ID) {

    companion object {
        const val ID: Short = 204
    }

    override fun toString(): String {
        return "SyncResponse($reconfiguration, data=${data.size})"
    }

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false

        other as SyncResponse

        if (reconfiguration != other.reconfiguration) return false
        if (!data.contentEquals(other.data)) return false

        return true
    }

    override fun hashCode(): Int {
        var result = reconfiguration.hashCode()
        result = 31 * result + data.contentHashCode()
        return result
    }

    object Serializer : ISerializer<SyncResponse> {
        override fun serialize(msg: SyncResponse, out: ByteBuf) {
            Reconfiguration.Serializer.serialize(msg.reconfiguration, out)
            out.writeInt(msg.data.size)
            out.writeBytes(msg.data)
        }

        override fun deserialize(buff: ByteBuf): SyncResponse {
            val reconfiguration = Reconfiguration.Serializer.deserialize(buff)
            val data = ByteArray(buff.readInt())
            buff.readBytes(data)
            return SyncResponse(reconfiguration, data)
        }
    }

}