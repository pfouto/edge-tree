package ipc

import pt.unl.fct.di.novasys.babel.generic.ProtoNotification
import java.net.Inet4Address

data class ActivateNotification(val contact: Inet4Address?) : ProtoNotification(ID) {
    companion object {
        const val ID: Short = 1
    }
}
class DeactivateNotification : ProtoNotification(ID) {
    companion object {
        const val ID: Short = 2
    }
}