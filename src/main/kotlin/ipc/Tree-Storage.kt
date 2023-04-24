package ipc

import pt.unl.fct.di.novasys.babel.generic.ProtoReply
import pt.unl.fct.di.novasys.babel.generic.ProtoRequest
import storage.DataObject
import storage.RemoteWrite

class ReplicationRequest(val partition: String, val key: String) : ProtoRequest(ID) {
    companion object {
        const val ID: Short = 201
    }
}

class ReplicationReply(val partition: String, val key: String, val obj: DataObject?) : ProtoReply(ID) {
    companion object {
        const val ID: Short = 202
    }
}

class PropagateWriteReply(val write: RemoteWrite) :ProtoReply(ID) {
    companion object {
        const val ID: Short = 203
    }
}

class PropagateWriteRequest(val id: Long, val write: RemoteWrite) : ProtoRequest(ID) {
    companion object {
        const val ID: Short = 204
    }
}

//TODO persistence parameter
class PersistenceUpdate(val persistenceMap: Map<Int, Long>) : ProtoReply(ID) {
    companion object {
        const val ID: Short = 205
    }
}
