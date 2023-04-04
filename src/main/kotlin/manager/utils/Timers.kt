package manager.utils

import pt.unl.fct.di.novasys.babel.generic.ProtoTimer

class BroadcastTimer : ProtoTimer(TIMER_ID) {
    override fun clone(): ProtoTimer {
        return this
    }

    companion object {
        const val TIMER_ID: Short = 101
    }
}

class TreeBuilderTimer : ProtoTimer(ID) {
    companion object {
        const val ID: Short = 102
    }

    override fun clone(): ProtoTimer {
        return this
    }
}
