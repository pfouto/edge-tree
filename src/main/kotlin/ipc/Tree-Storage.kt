package ipc

import pt.unl.fct.di.novasys.babel.generic.ProtoReply
import pt.unl.fct.di.novasys.babel.generic.ProtoRequest
import pt.unl.fct.di.novasys.network.data.Host
import storage.FetchedObject
import storage.ObjectIdentifier
import storage.RemoteWrite

/**
 * From Storage to Tree requesting object(s) to be fetched from a parent
 */
class LocalReplicationRequest(val objectIdentifiers: Set<ObjectIdentifier>) : ProtoRequest(ID) {
    companion object {
        const val ID: Short = 201
    }
}

/**
 * From Tree to Storage with objects(s) that has been previously requested locally
 */
class LocalReplicationReply(val objects: List<FetchedObject>) : ProtoReply(ID) {
    companion object {
        const val ID: Short = 202
    }
}

/**
 * From Tree to Storage requesting object(s) for a child
 */
class ChildReplicationRequest(val child: Host, val objectIdentifiers: Set<ObjectIdentifier>) : ProtoRequest(ID) {
    companion object {
        const val ID: Short = 203
    }
}

/**
 * From Storage to Tree with object(s) that has been previously requested by a child
 */
class ChildReplicationReply(val child: Host, val objects: List<FetchedObject>) :
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
