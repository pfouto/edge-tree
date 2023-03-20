package ipc

import proxy.utils.Operation
import pt.unl.fct.di.novasys.babel.generic.ProtoReply
import pt.unl.fct.di.novasys.babel.generic.ProtoRequest
import pt.unl.fct.di.novasys.network.data.Host
import tree.utils.HybridTimestamp

class OpRequest(val id: Long, val op: Operation) : ProtoRequest(ID) {
    companion object {
        const val ID: Short = 401
    }
}

class OpReply(val id: Long, val hlc: HybridTimestamp, val data: ByteArray?) : ProtoReply(ID) {
    companion object {
        const val ID: Short = 402
    }
}

class ClientWritePersistent(val id: Long) : ProtoReply(ID) {
    companion object {
        const val ID: Short = 403
    }
}

class TreeReconfigurationClients(val hosts: List<Host>) : ProtoReply(ID) {
    companion object {
        const val ID: Short = 403
    }
}

