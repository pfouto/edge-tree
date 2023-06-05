package ipc

import pt.unl.fct.di.novasys.babel.generic.ProtoReply
import pt.unl.fct.di.novasys.babel.generic.ProtoRequest
import pt.unl.fct.di.novasys.network.data.Host
import storage.*
import tree.messaging.up.SyncRequest
import tree.utils.WriteID

/**
 * From Tree to Storage requesting the data difference to send to synchronizing child
 */
data class DataDiffRequest(val child: Host, val msg: SyncRequest) : ProtoRequest(ID) {
    companion object {
        const val ID: Short = 216
    }
}

/**
 * From Storage to Tree with the data difference to send to synchronizing child
 */
data class DataDiffReply(val child: Host, val data: List<FetchedObject>) : ProtoReply(ID) {
    companion object {
        const val ID: Short = 215
    }
}

/**
 * From Tree to Storage requesting the current keys + metadata for parent synchronization
 */
data class FetchMetadataReq(val parent: Host) : ProtoRequest(ID) {
    companion object {
        const val ID: Short = 212
    }
}

/**
 * From Storage to Tree with the current keys + metadata for parent synchronization
 */
data class FetchMetadataRep(
    val parent: Host,
    val fullPartitions: MutableMap<String, Map<String, ObjectMetadata>>,
    val partialPartitions: MutableMap<String, Map<String, ObjectMetadata>>,
) : ProtoReply(ID) {
    companion object {
        const val ID: Short = 213
    }
}

/**
 * From Tree to Storage with a set of objects to apply after sync
 */
data class SyncApply(val objects: List<FetchedObject>) : ProtoRequest(ID) {
    companion object {
        const val ID: Short = 214
    }
}

/**
 * From Storage to Tree requesting object(s) to be fetched from a parent
 */
data class ObjReplicationReq(val requests: Set<ObjectIdentifier>) : ProtoRequest(ID) {

    constructor(singleRequest: ObjectIdentifier) : this(setOf(singleRequest))

    companion object {
        const val ID: Short = 201
    }
}

/**
 * From Tree to Storage with objects(s) that has been previously requested locally
 */
data class ObjReplicationRep(val objects: List<FetchedObject>) : ProtoReply(ID) {
    companion object {
        const val ID: Short = 202
    }
}

/**
 * From Storage to Tree requesting a full partition to be fetched from a parent
 */
data class PartitionReplicationReq(val partition: String) : ProtoRequest(ID) {
    companion object {
        const val ID: Short = 203
    }
}

/**
 * From Tree to Storage with a full partition that has been previously requested locally
 */
data class PartitionReplicationRep(val partition: String, val objects: List<Pair<String, ObjectData>>) : ProtoReply(ID) {
    companion object {
        const val ID: Short = 204
    }
}

/**
 * From Tree to Storage requesting object(s) for a child
 */
data class FetchObjectsReq(val child: Host, val objectIdentifiers: Set<ObjectIdentifier>) : ProtoRequest(ID) {
    companion object {
        const val ID: Short = 205
    }
}

/**
 * From Storage to Tree with object(s) that has been previously requested by a child
 */
data class FetchObjectsRep(val child: Host, val objects: List<FetchedObject>) :
    ProtoReply(ID) {
    companion object {
        const val ID: Short = 206
    }
}

/**
 * From Tree to Storage requesting object(s) for a child
 */
data class FetchPartitionReq(val child: Host, val partition: String) : ProtoRequest(ID) {
    companion object {
        const val ID: Short = 207
    }
}

/**
 * From Storage to Tree with object(s) that has been previously requested by a child
 */
data class FetchPartitionRep(val child: Host, val partition: String, val objects: List<Pair<String, ObjectData>>) :
    ProtoReply(ID) {
    companion object {
        const val ID: Short = 208
    }
}

/**
 * From Tree to Storage with a remote write to be applied locally
 */
data class PropagateWriteReply(val id: WriteID, val write: RemoteWrite) : ProtoReply(ID) {
    companion object {
        const val ID: Short = 209
    }
}

/**
 * From Storage to Tree with a local write to be propagated to the tree
 */
data class PropagateWriteRequest(val id: Long, val write: RemoteWrite) : ProtoRequest(ID) {
    companion object {
        const val ID: Short = 210
    }
}

data class PersistenceUpdate(val persistenceMap: Map<Int, Long>) : ProtoReply(ID) {
    companion object {
        const val ID: Short = 211
    }
}
