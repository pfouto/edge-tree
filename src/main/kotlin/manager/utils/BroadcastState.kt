package manager.utils

import manager.Manager
import pt.unl.fct.di.novasys.network.data.Host
import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import java.io.DataInputStream
import java.io.DataOutputStream
import java.net.Inet4Address

class BroadcastState(val host: Host, val location: Pair<Int, Int>, val resources: Int, val state: Manager.State) {

    override fun toString(): String {
        return "$host $location $resources $state"
    }

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false

        other as BroadcastState

        if (host != other.host) return false
        if (location != other.location) return false
        if (resources != other.resources) return false
        if (state != other.state) return false

        return true
    }

    fun toByteArray(): ByteArray {
        val baos = ByteArrayOutputStream()
        val dos = DataOutputStream(baos)
        dos.write(host.address.address)
        dos.writeInt(host.port)
        dos.writeInt(location.first)
        dos.writeInt(location.second)
        dos.writeInt(resources)
        dos.writeBoolean(state == Manager.State.ACTIVE)
        return baos.toByteArray()
    }

    companion object {
        fun fromByteArray(bytes: ByteArray): BroadcastState {
            val bais = ByteArrayInputStream(bytes)
            val dis = DataInputStream(bais)
            val addressBytes = ByteArray(4)
            dis.read(addressBytes)
            val host = Host(Inet4Address.getByAddress(addressBytes), dis.readInt())
            val location = Pair(dis.readInt(), dis.readInt())
            val resources = dis.readInt()
            val state = if (dis.readBoolean()) Manager.State.ACTIVE else Manager.State.INACTIVE
            return BroadcastState(host, location, resources, state)
        }
    }
}