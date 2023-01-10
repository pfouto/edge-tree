package tree.utils

import pt.unl.fct.di.novasys.babel.generic.ProtoNotification
import pt.unl.fct.di.novasys.network.data.Host

data class BootstrapNotification(val contact: Host?) : ProtoNotification(ID) {
    companion object {
        const val ID: Short = 201
    }
}