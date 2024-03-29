package hyparflood.utils

import pt.unl.fct.di.novasys.babel.generic.ProtoTimer
import pt.unl.fct.di.novasys.network.data.Host

class HelloTimeout : ProtoTimer(TIMER_ID) {
    override fun clone(): ProtoTimer {
        return this
    }

    companion object {
        const val TIMER_ID: Short = 301
    }
}

class JoinTimeout(val contact: Host) : ProtoTimer(TIMER_ID) {
    var count: Int = 1
        private set

    override fun clone(): ProtoTimer {
        return this
    }

    fun incCount() {
        count++
    }

    companion object {
        const val TIMER_ID: Short = 302
    }
}

class ShuffleTimeout : ProtoTimer(TIMER_ID) {
    override fun clone(): ProtoTimer {
        return this
    }

    companion object {
        const val TIMER_ID: Short = 303
    }
}