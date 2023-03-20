package tree.messaging.down

import io.netty.buffer.ByteBuf
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage
import pt.unl.fct.di.novasys.network.ISerializer

class Reject : ProtoMessage(ID) {
    companion object {
        const val ID: Short = 203
    }

    override fun toString(): String {
        return "Reject"
    }

    object Serializer : ISerializer<Reject> {
        override fun serialize(obj: Reject, buffer: ByteBuf) {
        }

        override fun deserialize(buffer: ByteBuf): Reject {
            return Reject()
        }
    }
}