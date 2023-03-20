package ipc

import manager.utils.BroadcastState
import pt.unl.fct.di.novasys.babel.generic.ProtoReply
import pt.unl.fct.di.novasys.babel.generic.ProtoRequest
import java.net.Inet4Address

class InitRequest(val address: Inet4Address?) : ProtoRequest(ID) {
    companion object {
        const val ID: Short = 301
    }
}

class BroadcastRequest(val payload: ByteArray) : ProtoRequest(ID) {
    companion object {
        const val ID: Short = 302
    }

    constructor(broadcastState: BroadcastState) :
            this(broadcastState.toByteArray())
}

class BroadcastReply(val payload: ByteArray) : ProtoReply(ID) {
    companion object {
        const val ID: Short = 301
    }
}