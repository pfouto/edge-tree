package ipc

import pt.unl.fct.di.novasys.babel.generic.ProtoNotification
import java.net.Inet4Address

data class ActivateNotification(val contact: Inet4Address?) : ProtoNotification(ID) {
    companion object {
        const val ID: Short = 201
    }
}

data class StateNotification(val active: Boolean) : ProtoNotification(ID) {
    companion object {
        const val ID: Short = 202
    }
}