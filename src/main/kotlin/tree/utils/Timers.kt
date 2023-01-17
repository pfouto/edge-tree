package tree.utils

import pt.unl.fct.di.novasys.babel.generic.ProtoTimer
import pt.unl.fct.di.novasys.network.data.Host

data class ReconnectTimer(val node: Host) : ProtoTimer(ID) {
    companion object {
        const val ID: Short = 201
    }

    override fun clone(): ProtoTimer {
        return this
    }
}

class PropagateTimer: ProtoTimer(ID) {
    companion object {
        const val ID: Short = 202
    }

    override fun clone(): ProtoTimer {
        return this
    }
}