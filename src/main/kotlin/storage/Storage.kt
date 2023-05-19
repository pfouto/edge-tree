package storage

import Config
import ipc.*
import org.apache.logging.log4j.LogManager
import proxy.ClientProxy
import proxy.utils.MigrationOperation
import proxy.utils.PartitionFetchOperation
import proxy.utils.ReadOperation
import proxy.utils.WriteOperation
import pt.unl.fct.di.novasys.babel.core.GenericProtocol
import pt.unl.fct.di.novasys.network.data.Host
import tree.TreeProto
import tree.utils.HybridTimestamp
import java.net.Inet4Address
import java.util.*
import kotlin.system.exitProcess

class Storage(val address: Inet4Address, private val config: Config) : GenericProtocol(NAME, ID) {

    //TODO if datacenter, persistence is instant and localData checks are not needed

    companion object {
        const val NAME = "Storage"
        const val ID: Short = 500

        const val CASSANDRA_TYPE = "cassandra"
        const val IN_MEMORY_TYPE = "in_memory"

        private val logger = LogManager.getLogger()
    }

    private val lww: Int = address.hashCode()

    private var storageWrapper: StorageWrapper? = null

    private val localData = DataIndex()

    // Pending reads and data requests for each pending object
    private val pendingObjects = mutableMapOf<ObjectIdentifier, Pair<MutableList<Long>, MutableList<Host>>>()

    // Pending data requests for each pending full partition (there are no reads on full partitions)
    private val pendingFullPartitions = mutableMapOf<String, MutableList<Host>>()


    private val pendingPersistence = mutableMapOf<Int, MutableList<Long>>()

    //Sorted list of pending migrations (sorted by timestamp and level)
    // TODO private val pendingMigrations =

    init {
        subscribeNotification(DeactivateNotification.ID) { _: DeactivateNotification, _ -> onDeactivate() }
        subscribeNotification(ActivateNotification.ID) { not: ActivateNotification, _ -> onActivate(not) }

        registerRequestHandler(OpRequest.ID) { req: OpRequest, _ -> onLocalOpRequest(req) }
        registerRequestHandler(FetchObjectsReq.ID) { req: FetchObjectsReq, _ -> onFetchObjectReq(req) }
        registerRequestHandler(FetchPartitionReq.ID) { req: FetchPartitionReq, _ -> onFetchPartitionReq(req) }
        registerReplyHandler(PropagateWriteReply.ID) { rep: PropagateWriteReply, _ -> onRemoteWrite(rep.write) }
        registerReplyHandler(ObjReplicationRep.ID) { rep: ObjReplicationRep, _ -> onObjReplicationReply(rep) }
        registerReplyHandler(PersistenceUpdate.ID) { rep: PersistenceUpdate, _ -> onPersistence(rep) }
        registerReplyHandler(PartitionReplicationRep.ID) { rep: PartitionReplicationRep, _ ->
            onPartitionReplicationReply(
                rep
            )
        }
    }

    override fun init(props: Properties) {

    }

    private fun onActivate(notification: ActivateNotification) {
        storageWrapper = if (notification.contact != null) //datacenter
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
        storageWrapper!!.initialize()
    }

    private fun onLocalOpRequest(req: OpRequest) {
        when (req.op) {
            is WriteOperation -> {
                val hlc = currentHlc()
                val objId = ObjectIdentifier(req.op.partition, req.op.key)
                val objData = ObjectData(req.op.value, hlc, lww)
                //Even if not present, we can complete the operation. After fetching the data,
                // LWW will converge to the correct value
                storageWrapper!!.put(objId, objData)
                sendReply(OpReply(req.id, hlc, null), ClientProxy.ID)

                //Check if available locally, if not, send replication request to tree
                if (!localData.containsObject(objId)) {
                    pendingObjects.computeIfAbsent(objId) {
                        sendRequest(ObjReplicationReq(Collections.singleton(objId)), TreeProto.ID)
                        Pair(mutableListOf(), mutableListOf())
                    }
                }

                //Ask tree to propagate write
                sendRequest(PropagateWriteRequest(req.id, RemoteWrite(objId, objData)), TreeProto.ID)

                //if persistence, also add to pending (different from read and migration pending)
                if (req.op.persistence > 0) {
                    if (req.op.persistence == 1.toShort())
                        sendReply(ClientWritePersistent(req.id), ClientProxy.ID)
                    else
                        pendingPersistence.computeIfAbsent(req.op.persistence.toInt()) { mutableListOf() }.add(req.id)
                }
            }

            is ReadOperation -> {
                //Check if available locally, if not, send replication request to tree
                val objId = ObjectIdentifier(req.op.partition, req.op.key)

                if (localData.containsObject(objId)) {
                    when (val data = storageWrapper!!.get(objId)) {
                        null -> sendReply(OpReply(req.id, null, null), ClientProxy.ID)
                        else -> sendReply(OpReply(req.id, data.hlc, data.value), ClientProxy.ID)
                    }
                } else {
                    pendingObjects.computeIfAbsent(objId) {
                        sendRequest(ObjReplicationReq(Collections.singleton(objId)), TreeProto.ID)
                        Pair(mutableListOf(), mutableListOf())
                    }.first.add(req.id)
                }
            }

            is PartitionFetchOperation -> {
                pendingFullPartitions.computeIfAbsent(req.op.partition) {
                    sendRequest(PartitionReplicationReq(req.op.partition), TreeProto.ID)
                    mutableListOf()
                }
            }

            is MigrationOperation -> {
                //TODO soonTM
                //Add to pending (different from read pending)
            }

            else -> assertOrExit(false, "Unknown operation type???")
        }
    }

    /**
     * A child node is requesting data objects
     */
    private fun onFetchObjectReq(req: FetchObjectsReq) {
        val toRequestParent = mutableSetOf<ObjectIdentifier>()
        val toRespond = mutableListOf<FetchedObject>()

        req.objectIdentifiers.forEach { objId ->
            if (localData.containsObject(objId)) {
                val dataObj = storageWrapper!!.get(objId)
                toRespond.add(FetchedObject(objId, dataObj))
            }

            pendingObjects.computeIfAbsent(objId) {
                toRequestParent.add(it)
                Pair(mutableListOf(), mutableListOf())
            }.second.add(req.child)
        }

        if (toRespond.isNotEmpty()) sendReply(FetchObjectsRep(req.child, toRespond), TreeProto.ID)
        if (toRequestParent.isNotEmpty()) sendRequest(ObjReplicationReq(toRequestParent), TreeProto.ID)
    }

    /**
     * A child node is requesting a full partition
     */
    private fun onFetchPartitionReq(req: FetchPartitionReq) {
        if (localData.containsFullPartition(req.partition)) {
            val partitionData = storageWrapper!!.getFullPartitionData(req.partition)
            sendReply(FetchPartitionRep(req.child, req.partition, partitionData), TreeProto.ID)
        } else {
            pendingFullPartitions.computeIfAbsent(req.partition) {
                sendRequest(PartitionReplicationReq(req.partition), TreeProto.ID)
                mutableListOf()
            }.add(req.child)
        }
    }

    private fun onObjReplicationReply(rep: ObjReplicationRep) {
        rep.objects.forEach {
            val newValue: ObjectData? = if (it.objectData != null)
                storageWrapper!!.put(it.objectIdentifier, it.objectData)
            else
                storageWrapper!!.get(it.objectIdentifier)

            val callbacks = pendingObjects.remove(it.objectIdentifier)!!

            //Client reads
            for (id in callbacks.first) {
                if (newValue == null) sendReply(OpReply(id, null, null), ClientProxy.ID)
                else sendReply(OpReply(id, newValue.hlc, newValue.value), ClientProxy.ID)
            }

            //Child object requests
            val childResponses = mutableMapOf<Host, MutableList<FetchedObject>>()
            for (child in callbacks.second) {
                childResponses.computeIfAbsent(child) { mutableListOf() }
                    .add(FetchedObject(it.objectIdentifier, newValue))
            }
            childResponses.forEach { (c, l) ->
                sendReply(FetchObjectsRep(c, l), TreeProto.ID)
            }

            localData.addObject(it.objectIdentifier)

        }
    }

    /**
     * A parent node sent us a requested data object
     */
    private fun onPartitionReplicationReply(rep: PartitionReplicationRep) {

        val result = localData.addFullPartition(rep.partition)
        assertOrExit(result, "Received partition replication for already existing full partition ${rep.partition}")
        rep.objects.forEach { (key, objData) ->
            storageWrapper!!.put(ObjectIdentifier(rep.partition, key), objData)
        }

        val callbacks = pendingFullPartitions.remove(rep.partition)!!
        if (callbacks.isNotEmpty()) {
            val partitionData = storageWrapper!!.getFullPartitionData(rep.partition)
            callbacks.forEach { sendReply(FetchPartitionRep(it, rep.partition, partitionData), TreeProto.ID) }
        }
    }

    private fun onRemoteWrite(write: RemoteWrite) {
        assertOrExit(
            localData.containsObject(write.objectIdentifier),
            "Received write for non-existent object ${write.objectIdentifier}"
        )
        storageWrapper!!.put(write.objectIdentifier, write.objectData)
    }

    /**
     * A persistence update was received from the tree
     */
    private fun onPersistence(rep: PersistenceUpdate) {
        rep.persistenceMap.forEach { (persistenceLevel, id) ->
            val pending = pendingPersistence[persistenceLevel]
            if (pending != null) {
                while (pending.isNotEmpty() && pending.first() <= id) {
                    sendReply(ClientWritePersistent(pending.removeFirst()), ClientProxy.ID)
                }
            }
        }
    }

    private fun currentHlc(): HybridTimestamp {
        //Maybe we don't need this... We just need the stable HLC received from the tree to know when to
        // execute remote ops... Maybe we do to tag writes...
        TODO("Not yet implemented")
    }

    private fun onDeactivate() {
        logger.info("Storage Deactivating (cleaning data)")

        localData.clear()
        pendingFullPartitions.clear()
        pendingObjects.clear()

        storageWrapper!!.cleanUp()
    }

    private fun assertOrExit(condition: Boolean, msg: String) {
        if (!condition) {
            logger.error(msg, AssertionError())
            exitProcess(1)
        }
    }
}




