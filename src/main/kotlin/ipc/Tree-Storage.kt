package ipc

import pt.unl.fct.di.novasys.babel.generic.ProtoReply
import pt.unl.fct.di.novasys.babel.generic.ProtoRequest
import pt.unl.fct.di.novasys.network.data.Host
import storage.DataObject
import storage.RemoteWrite

//Storage -> Tree
class LocalReplicationRequest(val partition: String, val key: String) : ProtoRequest(ID) {
    companion object {
        const val ID: Short = 201
    }
}

//Tree -> Storage
class LocalReplicationReply(val partition: String, val key: String, val obj: DataObject?) : ProtoReply(ID) {
    companion object {
        const val ID: Short = 202
    }
}

//Tree -> Storage
class ChildReplicationRequest(val child: Host, val partition: String, val key: String) : ProtoRequest(ID) {
    companion object {
        const val ID: Short = 203
    }
}

//Storage -> Tree
class ChildReplicationReply(val child: Host, val partition: String, val key: String, val obj: DataObject?) :
    ProtoReply(ID) {
    companion object {
        const val ID: Short = 204
    }
}

//Storage -> Tree
class PropagateWriteReply(val write: RemoteWrite) : ProtoReply(ID) {
    companion object {
        const val ID: Short = 205
    }
}

//Tree -> Storage
class PropagateWriteRequest(val id: Long, val write: RemoteWrite) : ProtoRequest(ID) {
    companion object {
        const val ID: Short = 206
    }
}

//Tree -> Storage
//TODO persistence parameter
class PersistenceUpdate(val persistenceMap: Map<Int, Long>) : ProtoReply(ID) {
    companion object {
        const val ID: Short = 207
    }
}
