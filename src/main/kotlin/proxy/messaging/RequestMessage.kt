package proxy.messaging

import io.netty.buffer.ByteBuf
import proxy.utils.Operation
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage
import pt.unl.fct.di.novasys.network.ISerializer

class RequestMessage(val id: Long, val op: Operation) : ProtoMessage(ID){

    companion object {
        const val ID: Short = 401
    }

    object Serializer : ISerializer<RequestMessage> {
        override fun serialize(msg: RequestMessage, out: ByteBuf) {
            out.writeLong(msg.id)
            Operation.Serializer.serialize(msg.op, out)
        }

        override fun deserialize(buff: ByteBuf): RequestMessage {
            val id = buff.readLong()
            val op: Operation = Operation.Serializer.deserialize(buff)
            return RequestMessage(id, op)
        }
    }

}