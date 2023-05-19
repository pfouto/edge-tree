package ipc

import pt.unl.fct.di.novasys.babel.generic.ProtoReply
import pt.unl.fct.di.novasys.babel.generic.ProtoRequest
import pt.unl.fct.di.novasys.network.data.Host
import storage.FetchedObject
import storage.ObjectData
import storage.ObjectIdentifier
import storage.RemoteWrite

/**
 * From Storage to Tree requesting object(s) to be fetched from a parent
 */
class ObjReplicationReq(val requests: Set<ObjectIdentifier>) : ProtoRequest(ID) {
    companion object {
        const val ID: Short = 201
    }
}
/**
 * From Tree to Storage with objects(s) that has been previously requested locally
 */
class ObjReplicationRep(val objects: List<FetchedObject>) : ProtoReply(ID) {
    companion object {
        const val ID: Short = 202
    }
}

/**
 * From Storage to Tree requesting a full partition to be fetched from a parent
 */
class PartitionReplicationReq(val request: String) : ProtoRequest(ID) {
    companion object {
        const val ID: Short = 203
    }
}

/**
 * From Tree to Storage with a full partition that has been previously requested locally
 */
class PartitionReplicationRep(val partition: String, val objects: List<Pair<String, ObjectData>>) : ProtoReply(ID) {
    companion object {
        const val ID: Short = 204
    }
}

/**
 * From Tree to Storage requesting object(s) for a child
 */
class FetchObjectsReq(val child: Host, val objectIdentifiers: Set<ObjectIdentifier>) : ProtoRequest(ID) {
    companion object {
        const val ID: Short = 205
    }
}

/**
 * From Storage to Tree with object(s) that has been previously requested by a child
 */
class FetchObjectsRep(val child: Host, val objects: List<FetchedObject>) :
    ProtoReply(ID) {
    companion object {
        const val ID: Short = 206
    }
}
/**
 * From Tree to Storage requesting object(s) for a child
 */
class FetchPartitionReq(val child: Host, val partition: String) : ProtoRequest(ID) {
    companion object {
        const val ID: Short = 207
    }
}

/**
 * From Storage to Tree with object(s) that has been previously requested by a child
 */
class FetchPartitionRep(val child: Host, val partition: String, val objects: List<Pair<String, ObjectData>>) :
    ProtoReply(ID) {
    companion object {
        const val ID: Short = 208
    }
}

/**
 * From Tree to Storage with a remote write to be applied locally
 */
class PropagateWriteReply(val write: RemoteWrite) : ProtoReply(ID) {
    companion object {
        const val ID: Short = 209
    }
}

/**
 * From Storage to Tree with a local write to be propagated to the tree
 */
class PropagateWriteRequest(val id: Long, val write: RemoteWrite) : ProtoRequest(ID) {
    companion object {
        const val ID: Short = 210
    }
}

//Tree -> Storage
//TODO persistence parameter
class PersistenceUpdate(val persistenceMap: Map<Int, Long>) : ProtoReply(ID) {
    companion object {
        const val ID: Short = 211
    }
}
