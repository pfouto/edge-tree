package engage.timers

import pt.unl.fct.di.novasys.babel.generic.ProtoTimer
import pt.unl.fct.di.novasys.network.data.Host

class ReconnectTimer(val node: Host) : ProtoTimer(TIMER_ID) {

    override fun clone(): ProtoTimer = this

    companion object {
        const val TIMER_ID: Short = 101
    }
}