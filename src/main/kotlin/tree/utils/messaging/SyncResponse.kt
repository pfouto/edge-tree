package tree.utils.messaging

import io.netty.buffer.ByteBuf
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage
import pt.unl.fct.di.novasys.network.ISerializer

data class SyncResponse(
    val downstream: Downstream,
    val data: ByteArray,
) : ProtoMessage(ID) {

    companion object {
        const val ID: Short = 201
    }

    override fun toString(): String {
        return "SyncResponse($downstream, data=${data.size})"
    }

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false

        other as SyncResponse

        if (downstream != other.downstream) return false
        if (!data.contentEquals(other.data)) return false

        return true
    }

    override fun hashCode(): Int {
        var result = downstream.hashCode()
        result = 31 * result + data.contentHashCode()
        return result
    }

    object Serializer : ISerializer<SyncResponse> {
        override fun serialize(msg: SyncResponse, out: ByteBuf) {
            Downstream.Serializer.serialize(msg.downstream, out)
            out.writeInt(msg.data.size)
            out.writeBytes(msg.data)
        }

        override fun deserialize(buff: ByteBuf): SyncResponse {
            val downstream = Downstream.Serializer.deserialize(buff)
            val data = ByteArray(buff.readInt())
            buff.readBytes(data)
            return SyncResponse(downstream, data)
        }
    }

}