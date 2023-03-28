package manager.utils

import manager.Manager
import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import java.io.DataInputStream
import java.io.DataOutputStream
import java.net.Inet4Address

class BroadcastState(val address: Inet4Address, val location: Pair<Int, Int>, val resources: Int, val state: Manager.State) {

    companion object {
        fun fromByteArray(bytes: ByteArray): BroadcastState {
            val bais = ByteArrayInputStream(bytes)
            val dis = DataInputStream(bais)
            val addressBytes = ByteArray(4)
            dis.read(addressBytes)
            val address = Inet4Address.getByAddress(addressBytes) as Inet4Address
            val location = Pair(dis.readInt(), dis.readInt())
            val resources = dis.readInt()
            val state = if (dis.readBoolean()) Manager.State.ACTIVE else Manager.State.INACTIVE
            return BroadcastState(address, location, resources, state)
        }
    }

    fun toByteArray(): ByteArray {
        val baos = ByteArrayOutputStream()
        val dos = DataOutputStream(baos)
        dos.write(address.address)
        dos.writeInt(location.first)
        dos.writeInt(location.second)
        dos.writeInt(resources)
        dos.writeBoolean(state == Manager.State.ACTIVE)
        return baos.toByteArray()
    }

    override fun toString(): String {
        return "${address.hostAddress} $location $resources $state"
    }

    override fun hashCode(): Int {
        var result = address.hashCode()
        result = 31 * result + location.hashCode()
        result = 31 * result + resources
        result = 31 * result + state.hashCode()
        return result
    }

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false

        other as BroadcastState

        if (address != other.address) return false
        if (location != other.location) return false
        if (resources != other.resources) return false
        if (state != other.state) return false

        return true
    }
}