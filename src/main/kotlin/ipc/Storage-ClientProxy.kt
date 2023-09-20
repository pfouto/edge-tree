package ipc

import engage.Clock
import pt.unl.fct.di.novasys.babel.generic.ProtoReply
import pt.unl.fct.di.novasys.babel.generic.ProtoRequest
import pt.unl.fct.di.novasys.network.data.Host
import tree.utils.HybridTimestamp

data class OpRequest(val proxyId: Long, val op: proxy.utils.Operation) : ProtoRequest(ID) {
    companion object {
        const val ID: Short = 401
    }
}

class OpReply(val proxyId: Long, val hlc: HybridTimestamp?, val data: ByteArray?) : ProtoReply(ID) {
    companion object {
        const val ID: Short = 402
    }
}

class ClientWritePersistent(val proxyId: Long) : ProtoReply(ID) {
    companion object {
        const val ID: Short = 403
    }
}

class TreeReconfigurationClients(val hosts: List<Host>) : ProtoReply(ID) {
    companion object {
        const val ID: Short = 404
    }
}

//ENGAGE

class EngageOpReply(val proxyId: Long, val clock: Clock?, val data: ByteArray?) : ProtoReply(ID) {
    companion object {
        const val ID: Short = 405
    }
}

data class EngageOpRequest(val proxyId: Long, val op: engage.Operation) : ProtoRequest(ID) {
    companion object {
        const val ID: Short = 406
    }
}
