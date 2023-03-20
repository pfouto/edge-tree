package ipc

import pt.unl.fct.di.novasys.babel.generic.ProtoNotification
import pt.unl.fct.di.novasys.network.data.Host

data class ActivateNotification(val contact: Host?) : ProtoNotification(ID) {
    companion object {
        const val ID: Short = 201
    }
}

data class StateNotification(val active: Boolean) : ProtoNotification(ID) {
    companion object {
        const val ID: Short = 202
    }
}