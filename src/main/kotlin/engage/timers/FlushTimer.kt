package engage.timers

import pt.unl.fct.di.novasys.babel.generic.ProtoTimer
import pt.unl.fct.di.novasys.network.data.Host

class FlushTimer(val edge: Host) : ProtoTimer(TIMER_ID) {

    override fun clone(): ProtoTimer = this
    override fun toString(): String {
        return "FlushTimer(edge=$edge)"
    }

    companion object {
        const val TIMER_ID: Short = 103
    }


}