package manager.utils

import pt.unl.fct.di.novasys.network.data.Host
import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import java.io.DataInputStream
import java.io.DataOutputStream
import java.net.Inet4Address

class BroadcastState(val address: Host, val location: Pair<Int, Int>, val resources: Int) {

    fun toByteArray(): ByteArray {
        val baos = ByteArrayOutputStream()
        val dos = DataOutputStream(baos)
        dos.write(address.address.address)
        dos.writeInt(address.port)
        dos.writeInt(location.first)
        dos.writeInt(location.second)
        dos.writeInt(resources)
        return baos.toByteArray()
    }

    companion object {
        fun fromByteArray(bytes: ByteArray): BroadcastState {
            val bais = ByteArrayInputStream(bytes)
            val dis = DataInputStream(bais)
            val addressBytes = ByteArray(4)
            dis.read(addressBytes)
            val address = Host(Inet4Address.getByAddress(addressBytes), dis.readInt())
            val location = Pair(dis.readInt(), dis.readInt())
            val resources = dis.readInt()
            return BroadcastState(address, location, resources)
        }
    }
}