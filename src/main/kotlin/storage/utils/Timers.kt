package storage.utils

import pt.unl.fct.di.novasys.babel.generic.ProtoTimer

class GarbageCollectTimer : ProtoTimer(ID) {
    companion object {
        const val ID: Short = 501
    }

    override fun clone(): ProtoTimer {
        return this
    }
}