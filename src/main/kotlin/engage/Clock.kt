package engage

import io.netty.buffer.ByteBuf
import pt.unl.fct.di.novasys.network.ISerializer
import java.net.InetAddress

class Clock (private val value : MutableMap<InetAddress, Int> = mutableMapOf()) {

    companion object {
        val serializer = object : ISerializer<Clock> {

            override fun serialize(clock: Clock, out: ByteBuf) {
                out.writeInt(clock.value.size)
                clock.value.forEach { (k, v) ->
                    out.writeBytes(k.address)
                    out.writeInt(v)
                }
            }

            override fun deserialize(input: ByteBuf): Clock {
                val mapSize = input.readInt()
                val value = mutableMapOf<InetAddress, Int>()
                for(i in 0 until mapSize){
                    val addrBytes = ByteArray(4)
                    input.readBytes(addrBytes)
                    val vUp = input.readInt()
                    value[InetAddress.getByAddress(addrBytes)] = vUp
                }
                return Clock(value)
            }
        }

    }

    override fun toString(): String {
        return "Clock($value)"
    }

    fun merge(other: Clock){
        other.value.forEach { (k, v) ->
            value.merge(k, v, ::maxOf)
        }
    }

    fun merge(key: InetAddress, newValue: Int){
        value.merge(key, newValue, ::maxOf)
    }

    fun getValue() : Map<InetAddress, Int>{
        return value
    }

}