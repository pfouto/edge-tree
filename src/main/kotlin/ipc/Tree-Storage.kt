package ipc

import pt.unl.fct.di.novasys.babel.generic.ProtoReply
import pt.unl.fct.di.novasys.babel.generic.ProtoRequest
import pt.unl.fct.di.novasys.network.data.Host
import storage.DataObject
import storage.RemoteWrite

/**
 * From Storage to Tree requesting a key to be fetched from a parent
 */
class LocalReplicationRequest(val partition: String, val key: String) : ProtoRequest(ID) {
    companion object {
        const val ID: Short = 201
    }
}

/**
 * From Tree to Storage with a key that has been previously requested locally
 */
class LocalReplicationReply(val partition: String, val key: String, val obj: DataObject?) : ProtoReply(ID) {
    companion object {
        const val ID: Short = 202
    }
}

/**
 * From Tree to Storage requesting a key for a child
 */
class ChildReplicationRequest(val child: Host, val partition: String, val key: String) : ProtoRequest(ID) {
    companion object {
        const val ID: Short = 203
    }
}

/**
 * From Storage to Tree with a key that has been previously requested by a child
 */
class ChildReplicationReply(val child: Host, val partition: String, val key: String, val obj: DataObject?) :
    ProtoReply(ID) {
    companion object {
        const val ID: Short = 204
    }
}

/**
 * From Tree to Storage with a remote write to be applied locally
 */
class PropagateWriteReply(val write: RemoteWrite) : ProtoReply(ID) {
    companion object {
        const val ID: Short = 205
    }
}

/**
 * From Storage to Tree with a local write to be propagated to the tree
 */
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
