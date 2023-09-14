package engage.messaging

import deserializeString
import engage.Clock
import io.netty.buffer.ByteBuf
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage
import pt.unl.fct.di.novasys.network.ISerializer
import serializeString
import java.net.InetAddress

class UpdateNot(
    val source: InetAddress, val vUp: Int, val part: String, val vc: Clock,
    val data: ByteArray?, val mf: MetadataFlush?,
) : ProtoMessage(MSG_ID) {

    companion object {
        const val MSG_ID: Short = 202

        val serializer = object : ISerializer<UpdateNot> {

            override fun serialize(msg: UpdateNot, out: ByteBuf) {
                out.writeBytes(msg.source.address)
                out.writeInt(msg.vUp)
                serializeString(msg.part, out)
                Clock.serializer.serialize(msg.vc, out)
                if (msg.data != null) {
                    out.writeInt(msg.data.size)
                    out.writeBytes(msg.data)
                } else {
                    out.writeInt(0)
                }
                if (msg.mf != null) {
                    out.writeBoolean(true)
                    MetadataFlush.serializer.serialize(msg.mf, out)
                } else {
                    out.writeBoolean(false)
                }
            }

            override fun deserialize(input: ByteBuf): UpdateNot {
                val addrBytes = ByteArray(4)
                input.readBytes(addrBytes)
                val vUp: Int = input.readInt()
                val partition: String = deserializeString(input)
                val clock = Clock.serializer.deserialize(input)
                val dataSize: Int = input.readInt()
                val data: ByteArray?
                if (dataSize > 0) {
                    data = ByteArray(dataSize)
                    input.readBytes(data)
                } else data = null
                val mf: MetadataFlush?
                val mfPresent: Boolean = input.readBoolean()
                mf = if (mfPresent) MetadataFlush.serializer.deserialize(input) else null
                return UpdateNot(InetAddress.getByAddress(addrBytes), vUp, partition, clock, data, mf)
            }
        }
    }

    override fun toString(): String {
        return "UpdtNot(source=${source.hostAddress}, vUp=$vUp, part='$part', vc=$vc${if(data != null)
            ", data=${data.size}" else ""}${if(mf!=null) ", mf=$mf" else ""})"
    }

    fun copyMergingMF(newMF: MetadataFlush): UpdateNot {
        if(mf != null) newMF.merge(mf)
        return UpdateNot(source, vUp, part, vc, data, newMF)
    }

}