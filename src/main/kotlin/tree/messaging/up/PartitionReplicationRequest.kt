package tree.messaging.up

import decodeUTF8
import encodeUTF8
import io.netty.buffer.ByteBuf
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage
import pt.unl.fct.di.novasys.network.ISerializer

data class PartitionReplicationRequest(val partition: String) : ProtoMessage(ID) {

    companion object {
        const val ID: Short = 212
    }

    object Serializer : ISerializer<PartitionReplicationRequest> {
        override fun serialize(msg: PartitionReplicationRequest, out: ByteBuf) {
            encodeUTF8(msg.partition, out)
        }

        override fun deserialize(buff: ByteBuf): PartitionReplicationRequest {
            return PartitionReplicationRequest(decodeUTF8(buff))
        }
    }
}