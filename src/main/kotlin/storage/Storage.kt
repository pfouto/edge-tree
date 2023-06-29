package storage

import Config
import getTimeMillis
import ipc.*
import org.apache.logging.log4j.LogManager
import proxy.ClientProxy
import proxy.utils.MigrationOperation
import proxy.utils.PartitionFetchOperation
import proxy.utils.ReadOperation
import proxy.utils.WriteOperation
import pt.unl.fct.di.novasys.babel.core.GenericProtocol
import pt.unl.fct.di.novasys.network.data.Host
import storage.utils.ChildDataIndex
import storage.utils.DataIndex
import storage.utils.GarbageCollectTimer
import storage.utils.LogNObjectsTimer
import storage.wrappers.CassandraWrapper
import storage.wrappers.InMemoryWrapper
import tree.TreeProto
import tree.utils.ChildReady
import tree.utils.HybridTimestamp
import tree.utils.WriteID
import java.net.Inet4Address
import java.util.*
import kotlin.system.exitProcess

class Storage(val address: Inet4Address, private val config: Config) : GenericProtocol(NAME, ID) {

    companion object {
        const val NAME = "Storage"
        const val ID: Short = 500

        const val CASSANDRA_TYPE = "cassandra"
        const val IN_MEMORY_TYPE = "in_memory"

        private val logger = LogManager.getLogger()
    }

    private val lww: Int = address.hashCode()

    private lateinit var storageWrapper: StorageWrapper

    private lateinit var dataIndex: DataIndex

    private val childData: MutableMap<Host, ChildReady> = mutableMapOf()

    private var amDc: Boolean = false

    //Self
    private val localTimeLock = Object()

    @Volatile
    private var localTime: HybridTimestamp = HybridTimestamp(getTimeMillis(), 0)

    data class PendingObject(val reads: MutableList<Long> = mutableListOf(),
                             val writes: MutableList<Pair<PropagateWriteRequest, Short>> = mutableListOf(),
                             val hosts: MutableList<Host> = mutableListOf()
    )
    // Pending reads/writes and data requests for each pending object
    private val pendingObjects = mutableMapOf<ObjectIdentifier, PendingObject>()

    // Pending data requests for each pending full partition (there are no reads on full partitions)
    private val pendingFullPartitions = mutableMapOf<String, MutableList<Host>>()

    private val pendingPersistence = mutableMapOf<Int, MutableList<PropagateWriteRequest>>()

    private var nOps = 0;

    init {
        subscribeNotification(DeactivateNotification.ID) { _: DeactivateNotification, _ -> onDeactivate() }
        subscribeNotification(ActivateNotification.ID) { not: ActivateNotification, _ -> onActivate(not) }

        registerRequestHandler(OpRequest.ID) { req: OpRequest, _ -> onLocalOpRequest(req) }
        registerRequestHandler(FetchObjectsReq.ID) { req: FetchObjectsReq, _ -> onFetchObjectReq(req) }
        registerRequestHandler(FetchPartitionReq.ID) { req: FetchPartitionReq, _ -> onFetchPartitionReq(req) }
        registerReplyHandler(PropagateWriteReply.ID) { rep: PropagateWriteReply, _ -> onRemoteWrite(rep.id, rep.write, rep.downstream) }
        registerReplyHandler(ObjReplicationRep.ID) { rep: ObjReplicationRep, _ -> onObjReplicationReply(rep) }
        registerReplyHandler(PersistenceUpdate.ID) { rep: PersistenceUpdate, _ -> onPersistence(rep) }
        registerReplyHandler(PartitionReplicationRep.ID) { rep: PartitionReplicationRep, _ ->
            onPartitionReplicationReply(rep)
        }
        registerRequestHandler(FetchMetadataReq.ID) { req: FetchMetadataReq, _ -> onFetchMetadata(req) }
        registerRequestHandler(SyncApply.ID) { req: SyncApply, _ -> onSyncApply(req) }
        registerRequestHandler(DataDiffRequest.ID) { req: DataDiffRequest, _ -> onDataDiffRequest(req) }
        registerRequestHandler(ReconfigurationApply.ID) { req: ReconfigurationApply, _ -> onReconfiguration(req) }
        registerRequestHandler(AddedChildRequest.ID) { req: AddedChildRequest, _ -> onAddedChild(req) }
        registerRequestHandler(RemovedChildRequest.ID) { req: RemovedChildRequest, _ -> onRemovedChild(req) }
        registerReplyHandler(MigrationReply.ID) { rep: MigrationReply, _ -> onMigrationReply(rep) }

        registerTimerHandler(GarbageCollectTimer.ID) { _: GarbageCollectTimer, _ -> onGarbageCollect() }
        registerTimerHandler(LogNObjectsTimer.ID) { _: LogNObjectsTimer, _ -> onLogNObjects() }
        Runtime.getRuntime().addShutdownHook(Thread { logger.info("$nOps $dataIndex") })

    }

    override fun init(props: Properties) {

    }

    private fun onActivate(notification: ActivateNotification) {

        amDc = notification.contact == null

        dataIndex = when {
            !amDc -> DataIndex()
            else -> DataIndex.DCDataIndex()
        }

        if (!amDc)
            setupPeriodicTimer(GarbageCollectTimer(), config.gc_period, config.gc_period)

        storageWrapper = if (amDc) //datacenter
            when (config.dc_storage_type) {
                CASSANDRA_TYPE -> CassandraWrapper()
                IN_MEMORY_TYPE -> InMemoryWrapper()
                else -> {
                    logger.error("Invalid storage type: ${config.dc_storage_type}")
                    exitProcess(1)
                }
            }
        else { // node
            when (config.node_storage_type) {
                CASSANDRA_TYPE -> CassandraWrapper()
                IN_MEMORY_TYPE -> InMemoryWrapper()
                else -> {
                    logger.error("Invalid storage type: ${config.node_storage_type}")
                    exitProcess(1)
                }
            }
        }

        if(config.log_n_objects > 0) {
            setupPeriodicTimer(LogNObjectsTimer(), config.log_n_objects, config.log_n_objects)
        }

        storageWrapper.initialize()
    }

    private fun onFetchMetadata(req: FetchMetadataReq) {
        logger.debug("Received FetchMetadataReq to parent {}", req.parent)
        val fullPartitions = mutableMapOf<String, Map<String, ObjectMetadata>>()
        val partialPartitions = mutableMapOf<String, Map<String, ObjectMetadata>>()
        dataIndex.partitionIterator().forEach { partition ->
            when (partition) {
                is DataIndex.FullPartition -> {
                    fullPartitions[partition.name] = storageWrapper.getFullPartitionMetadata(partition.name)
                }

                is DataIndex.PartialPartition -> {
                    val partialPartition = mutableMapOf<String, ObjectMetadata>()
                    partition.keyIterator().forEach { key ->
                        partialPartition[key] =
                            storageWrapper.getMetadata(ObjectIdentifier(partition.name, key)) ?: ObjectMetadata(
                                HybridTimestamp(),
                                0
                            )
                    }
                    partialPartitions[partition.name] = partialPartition
                }
            }

        }
        sendReply(FetchMetadataRep(req.parent, fullPartitions, partialPartitions), TreeProto.ID)

    }

    private fun onDataDiffRequest(req: DataDiffRequest) {
        logger.debug("Diff data request from {}", req.child)
        val response: MutableList<FetchedObject> = mutableListOf()
        req.msg.fullPartitions.forEach { p ->
            assertOrExit(dataIndex.containsFullPartition(p.key), "Partition ${p.key} not found")
            response.addAll(storageWrapper.getPartitionDataIfNewer(p.key, p.value))
        }
        req.msg.partialPartitions.forEach { (pName, objects) ->
            objects.forEach { (key, metadata) ->
                assertOrExit(dataIndex.containsObject(ObjectIdentifier(pName, key)), "Object $key not found")
                val objId = ObjectIdentifier(pName, key)
                val objData = storageWrapper.get(objId)
                if (objData != null && objData.metadata.isAfter(metadata))
                    response.add(FetchedObject(objId, objData))
            }
        }
        sendReply(DataDiffReply(req.child, response), TreeProto.ID)
    }

    private fun onSyncApply(req: SyncApply) {
        logger.debug("Sync apply received")

        //Apply remote data locally
        req.objects.forEach { (objId, objData) ->
            storageWrapper.put(objId, objData!!)
        }
        //Re-request pending data
        pendingFullPartitions.forEach { (p, _) ->
            sendRequest(PartitionReplicationReq(p), TreeProto.ID)
        }
        sendRequest(ObjReplicationReq(pendingObjects.keys.toSet()), TreeProto.ID)

        //Re-send writes regarding pending persistence requests
        pendingPersistence.forEach { (_, reqList) -> reqList.forEach { req -> sendRequest(req, TreeProto.ID) } }

    }

    private fun onLocalOpRequest(req: OpRequest) {
        logger.debug("Received local op request {}", req)
        when (req.op) {
            is WriteOperation -> {
                val hlc = getTimestamp()
                val objId = ObjectIdentifier(req.op.partition, req.op.key)
                val objData = ObjectData(req.op.value, ObjectMetadata(hlc, lww))
                //Even if not present, we can complete the operation. After fetching the data,
                // LWW will converge to the correct value
                storageWrapper.put(objId, objData)
                nOps++

                //TODO print id for log purposes (without persistence id)

                sendReply(OpReply(req.id, hlc, null), ClientProxy.ID)

                val propagateWriteRequest = PropagateWriteRequest(req.id, RemoteWrite(objId, objData))

                //Check if available locally, if not, send replication request to tree
                if (dataIndex.containsObject(objId)) {
                    dataIndex.updateTimestamp(objId)
                    //Ask tree to propagate write
                    sendRequest(propagateWriteRequest, TreeProto.ID)
                    //if persistence, also add to pending (different from read and migration pending)
                    if (req.op.persistence > 0) {
                        if (req.op.persistence == 1.toShort() || amDc)
                            sendReply(ClientWritePersistent(req.id), ClientProxy.ID)
                        else
                            pendingPersistence.computeIfAbsent(req.op.persistence.toInt()) { mutableListOf() }
                                .add(propagateWriteRequest)
                    }
                } else {
                    pendingObjects.computeIfAbsent(objId) {
                        sendRequest(ObjReplicationReq(objId), TreeProto.ID)
                        logger.debug("Requesting {} from tree", objId)
                        PendingObject()
                    }.writes.add(Pair(propagateWriteRequest, req.op.persistence))
                }
            }

            is ReadOperation -> {
                //Check if available locally, if not, send replication request to tree
                val objId = ObjectIdentifier(req.op.partition, req.op.key)

                if (dataIndex.containsObject(objId)) {
                    dataIndex.updateTimestamp(objId)
                    logger.debug("Read {} served locally", req.id)
                    when (val data = storageWrapper.get(objId)) {
                        null -> sendReply(OpReply(req.id, null, null), ClientProxy.ID)
                        else -> sendReply(OpReply(req.id, data.metadata.hlc, data.value), ClientProxy.ID)
                    }
                } else {
                    pendingObjects.computeIfAbsent(objId) {
                        sendRequest(ObjReplicationReq(objId), TreeProto.ID)
                        logger.debug("Requesting {} from tree", objId)
                        PendingObject()
                    }.reads.add(req.id)
                    logger.debug("Read {} added to pending", req.id)
                }
            }

            is PartitionFetchOperation -> {
                pendingFullPartitions.computeIfAbsent(req.op.partition) {
                    sendRequest(PartitionReplicationReq(req.op.partition), TreeProto.ID)
                    mutableListOf()
                }
                sendReply(OpReply(req.id, null, null), ClientProxy.ID)
            }

            is MigrationOperation -> {
                sendRequest(MigrationRequest(req.id, req.op), TreeProto.ID)
            }

            else -> assertOrExit(false, "Unknown operation type???")
        }
    }

    private fun onMigrationReply(rep: MigrationReply) {
        sendReply(OpReply(rep.id, null, null), ClientProxy.ID)
    }

    /**
     * A child node is requesting data objects
     */
    private fun onFetchObjectReq(req: FetchObjectsReq) {
        val toRequestParent = mutableSetOf<ObjectIdentifier>()
        val toRespond = mutableListOf<FetchedObject>()

        req.objectIdentifiers.forEach { objId ->
            if (dataIndex.containsObject(objId)) {
                val dataObj = storageWrapper.get(objId)
                toRespond.add(FetchedObject(objId, dataObj))
            } else {
                pendingObjects.computeIfAbsent(objId) {
                    toRequestParent.add(it)
                    PendingObject()
                }.hosts.add(req.child)
            }
        }

        if (toRespond.isNotEmpty()) sendReply(FetchObjectsRep(req.child, toRespond), TreeProto.ID)
        if (toRequestParent.isNotEmpty()) {
            logger.debug("Requesting {} from tree", toRequestParent)
            sendRequest(ObjReplicationReq(toRequestParent), TreeProto.ID)
        }
    }

    /**
     * A child node is requesting a full partition
     */
    private fun onFetchPartitionReq(req: FetchPartitionReq) {
        if (dataIndex.containsFullPartition(req.partition)) {
            val partitionData = storageWrapper.getFullPartitionData(req.partition)
            sendReply(FetchPartitionRep(req.child, req.partition, partitionData), TreeProto.ID)
        } else {
            pendingFullPartitions.computeIfAbsent(req.partition) {
                sendRequest(PartitionReplicationReq(req.partition), TreeProto.ID)
                mutableListOf()
            }.add(req.child)
        }
    }

    /**
     * A parent node sent us a requested data object
     */
    private fun onObjReplicationReply(rep: ObjReplicationRep) {
        logger.debug("Received object replication reply {}", rep)
        rep.objects.forEach {
            val newValue: ObjectData? = if (it.objectData != null)
                storageWrapper.put(it.objectIdentifier, it.objectData)
            else
                storageWrapper.get(it.objectIdentifier)

            val callbacks = pendingObjects.remove(it.objectIdentifier)!!

            //Client reads
            for (id in callbacks.reads) {
                logger.debug("Sending read reply to {}", id)
                if (newValue == null) sendReply(OpReply(id, null, null), ClientProxy.ID)
                else sendReply(OpReply(id, newValue.metadata.hlc, newValue.value), ClientProxy.ID)
            }

            //Client writes
            for((propagateWriteRequest, persistence) in callbacks.writes){
                logger.debug("Propagating pending write {} {}", propagateWriteRequest, persistence)
                sendRequest(propagateWriteRequest, TreeProto.ID)
                //if persistence, also add to pending (different from read and migration pending)
                if (persistence > 0) {
                    if (persistence == 1.toShort() || amDc)
                        sendReply(ClientWritePersistent(propagateWriteRequest.id), ClientProxy.ID)
                    else
                        pendingPersistence.computeIfAbsent(persistence.toInt()) { mutableListOf() }
                            .add(propagateWriteRequest)
                }

            }

            //Child object requests
            val childResponses = mutableMapOf<Host, MutableList<FetchedObject>>()
            for (child in callbacks.hosts) {
                childResponses.computeIfAbsent(child) { mutableListOf() }
                    .add(FetchedObject(it.objectIdentifier, newValue))
            }
            childResponses.forEach { (c, l) ->
                sendReply(FetchObjectsRep(c, l), TreeProto.ID)
            }

            dataIndex.addObject(it.objectIdentifier)

        }
    }

    private fun onPartitionReplicationReply(rep: PartitionReplicationRep) {

        val result = dataIndex.addFullPartition(rep.partition)
        assertOrExit(result, "Received partition replication for already existing full partition ${rep.partition}")
        rep.objects.forEach { (key, objData) ->
            storageWrapper.put(ObjectIdentifier(rep.partition, key), objData)
        }

        val callbacks = pendingFullPartitions.remove(rep.partition)!!
        if (callbacks.isNotEmpty()) {
            val partitionData = storageWrapper.getFullPartitionData(rep.partition)
            callbacks.forEach { sendReply(FetchPartitionRep(it, rep.partition, partitionData), TreeProto.ID) }
        }
    }

    private fun onRemoteWrite(id: WriteID, write: RemoteWrite, downstream: Boolean) {
        if (!dataIndex.containsObject(write.objectIdentifier) && !pendingObjects.containsKey(write.objectIdentifier)) {
            if(downstream){
                logger.warn("Ignoring downstream write for non existent object ${write.objectIdentifier}")
            } else {
                logger.error("Received upstream write for non-existent object ${write.objectIdentifier}")
                exitProcess(1)
            }
            return
        }

        synchronized(localTimeLock) {
            localTime = localTime.mergeTimestamp(write.objectData.metadata.hlc)
        }

        //TODO print id for log purposes (without persistence id)

        //If we used a slower (replicated/to disk) storage, we could tag write operations with the client dependency
        //to know when we can safely execute operations in parallel. Here we do them serially, so we don't need to.
        //We do not update the dataindex timestamp here, only on local writes/reads!
        nOps++
        storageWrapper.put(write.objectIdentifier, write.objectData)
    }

    private fun onReconfiguration(req: ReconfigurationApply) {
        //No need to re-send requests partitions/objects, since we do that when handling the SyncApply
        //No need to re-send pending persistence operations, since we do that when handling the SyncApply

        sendReply(TreeReconfigurationClients(req.branch), ClientProxy.ID)

    }

    private fun onRemovedChild(req: RemovedChildRequest) {
        childData.remove(req.child)
    }

    private fun onAddedChild(req: AddedChildRequest) {
        childData[req.child] = req.data
    }

    private fun onGarbageCollect() {
        val (removedObjects, removedPartitions) =
            dataIndex.garbageCollect(System.currentTimeMillis(), config.gc_threshold, childData)
        removedObjects.forEach { storageWrapper.delete(it) }
        removedPartitions.forEach { storageWrapper.deletePartition(it) }
        logger.info("Garbage collected ${removedObjects.size} objects and ${removedPartitions.size} partitions")
        if((removedObjects.isNotEmpty() || removedPartitions.isNotEmpty()) && logger.isDebugEnabled) {
            logger.debug("Garbage collected objects: $removedObjects")
            logger.debug("Garbage collected partitions: $removedPartitions")
        }
        sendRequest(RemoveReplicasRequest(removedObjects, removedPartitions), TreeProto.ID)
    }

    private fun onLogNObjects(){
        logger.info("nobjects: ${dataIndex.nObjects()}")
    }

    /**
     * A persistence update was received from the tree
     */
    private fun onPersistence(rep: PersistenceUpdate) {
        assertOrExit(!amDc, "Received persistence update in DC mode")

        rep.persistenceMap.forEach { (persistenceLevel, id) ->
            if (persistenceLevel == Int.MAX_VALUE) {
                pendingPersistence.forEach { (_, opList) ->
                    while (opList.isNotEmpty() && opList.first().id <= id) {
                        sendReply(ClientWritePersistent(opList.removeFirst().id), ClientProxy.ID)
                    }
                }
            } else {
                val pending = pendingPersistence[persistenceLevel]
                if (pending != null) {
                    while (pending.isNotEmpty() && pending.first().id <= id) {
                        sendReply(ClientWritePersistent(pending.removeFirst().id), ClientProxy.ID)
                    }
                }
            }
        }
    }

    fun getTimestamp(): HybridTimestamp {
        synchronized(localTimeLock) {
            localTime = localTime.nextTimestamp()
            return localTime
        }
    }

    private fun onDeactivate() {
        logger.info("Storage Deactivating (cleaning data)")

        dataIndex.clear()
        pendingFullPartitions.clear()
        pendingObjects.clear()

        storageWrapper.cleanUp()
    }

    private fun assertOrExit(condition: Boolean, msg: String) {
        if (!condition) {
            logger.error(msg, AssertionError())
            exitProcess(1)
        }
    }
}




