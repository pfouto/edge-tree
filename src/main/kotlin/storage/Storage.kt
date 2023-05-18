package storage

import Config
import ipc.*
import org.apache.logging.log4j.LogManager
import proxy.ClientProxy
import proxy.utils.MigrationOperation
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

    private val localData = mutableMapOf<String, MutableMap<String, ObjectState>>()

    private val pendingPersistence = mutableMapOf<Int, MutableList<Long>>()

    //Sorted list of pending migrations (sorted by timestamp and level)
    // TODO private val pendingMigrations =

    init {
        subscribeNotification(DeactivateNotification.ID) { _: DeactivateNotification, _ -> onDeactivate() }
        subscribeNotification(ActivateNotification.ID) { not: ActivateNotification, _ -> onActivate(not) }

        registerRequestHandler(OpRequest.ID) { req: OpRequest, _ -> onLocalOpRequest(req) }
        registerRequestHandler(ChildReplicationRequest.ID) { req: ChildReplicationRequest, _ -> onChildReplication(req) }
        registerReplyHandler(PropagateWriteReply.ID) { rep: PropagateWriteReply, _ -> onRemoteWrite(rep.write) }
        registerReplyHandler(LocalReplicationReply.ID) { rep: LocalReplicationReply, _ -> onReplicationReply(rep) }
        registerReplyHandler(PersistenceUpdate.ID) { rep: PersistenceUpdate, _ -> onPersistence(rep) }
    }


    override fun init(props: Properties) {

    }

    private fun onActivate(notification: ActivateNotification) {
        storageWrapper = if (notification.contact != null) //datacenter
            if (config.dc_storage_type == CASSANDRA_TYPE) CassandraWrapper() else InMemoryWrapper()
        else // node
            if (config.node_storage_type == CASSANDRA_TYPE) CassandraWrapper() else InMemoryWrapper()
        storageWrapper!!.initialize()
    }

    /**
     * A child node is requesting a data object
     */
    private fun onChildReplication(req: ChildReplicationRequest) {

        val toRequestParent = mutableSetOf<ObjectIdentifier>()
        val toRespond = mutableListOf<FetchedObject>()

        req.objectIdentifiers.forEach {
            val partition = localData.computeIfAbsent(it.partition) { mutableMapOf() }
            when (val obj = partition[it.key]) {
                null -> {
                    toRequestParent.add(it)
                    val newState = Pending()
                    newState.pendingReplications.add(req.child)
                    partition[it.key] = newState
                }

                is Pending -> {
                    obj.pendingReplications.add(req.child)
                }

                is Present -> {
                    val dataObj = storageWrapper!!.get(it)
                    toRespond.add(FetchedObject(it, dataObj))
                }

            }
        }
        sendReply(ChildReplicationReply(req.child, toRespond), TreeProto.ID)
        sendRequest(LocalReplicationRequest(toRequestParent), TreeProto.ID)
    }

    /**
     * A parent node sent us a requested data object
     */
    private fun onReplicationReply(rep: LocalReplicationReply) {

        rep.objects.forEach {
            val newValue: DataObject? = if (it.dataObject != null)
                storageWrapper!!.put(it.objectIdentifier, it.dataObject)
            else
                storageWrapper!!.get(it.objectIdentifier)

            val partition = localData[it.objectIdentifier.partition]!!
            val pending = partition[it.objectIdentifier.key]!! as Pending

            for (id in pending.pendingReads) {
                if (newValue == null) sendReply(OpReply(id, null, null), ClientProxy.ID)
                else sendReply(OpReply(id, newValue.hlc, newValue.value), ClientProxy.ID)
            }

            val childResponses = mutableMapOf<Host, MutableList<FetchedObject>>()
            for (child in pending.pendingReplications) {
                childResponses.computeIfAbsent(child) { mutableListOf() }
                    .add(FetchedObject(it.objectIdentifier, newValue))
            }
            partition[it.objectIdentifier.key] = Present()

            childResponses.forEach { (c, l) ->
                sendReply(ChildReplicationReply(c, l), TreeProto.ID)
            }
        }
    }

    private fun onRemoteWrite(write: RemoteWrite) {
        assertOrExit(
            localData[write.objectIdentifier.partition]?.get(write.objectIdentifier.key) != null,
            "Received write for non-existent object ${write.objectIdentifier}"
        )
        storageWrapper!!.put(write.objectIdentifier, write.dataObject)
    }

    private fun onLocalOpRequest(req: OpRequest) {
        when (req.op) {
            is WriteOperation -> {
                val hlc = currentHlc()
                val objId = ObjectIdentifier(req.op.partition, req.op.key)
                val objData = DataObject(req.op.value, hlc, lww)
                storageWrapper!!.put(objId, objData)
                sendReply(OpReply(req.id, hlc, null), ClientProxy.ID)

                //Check if available locally, if not, send replication request to tree
                val partition = localData.computeIfAbsent(req.op.partition) { mutableMapOf() }
                partition.computeIfAbsent(req.op.key) {
                    sendRequest(LocalReplicationRequest(Collections.singleton(objId)), TreeProto.ID)
                    Pending()
                }

                //Ask tree to propagate write
                sendRequest(
                    PropagateWriteRequest(req.id, RemoteWrite(objId, objData)),
                    TreeProto.ID
                )

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
                val partition = localData.computeIfAbsent(objId.partition) { mutableMapOf() }
                val objState = partition.computeIfAbsent(objId.key) {
                    sendRequest(LocalReplicationRequest(Collections.singleton(objId)), TreeProto.ID)
                    Pending()
                }

                //Execute if present, or add to pending if not
                when (objState) {
                    is Present -> {
                        when (val data = storageWrapper!!.get(objId)) {
                            null -> sendReply(OpReply(req.id, null, null), ClientProxy.ID)
                            else -> sendReply(OpReply(req.id, data.hlc, data.value), ClientProxy.ID)
                        }
                    }

                    is Pending -> objState.pendingReads.add(req.id)
                }

            }

            is MigrationOperation -> {
                //TODO soonTM
                //Add to pending (different from read pending)
            }
        }
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
        TODO("Not yet implemented")
    }

    private fun onDeactivate() {
        logger.info("Storage Deactivating (cleaning data)")
        localData.forEach { (_, u) -> u.clear() }
        localData.clear()
        storageWrapper!!.cleanUp()
    }

    private fun assertOrExit(condition: Boolean, msg: String) {
        if (!condition) {
            logger.error(msg, AssertionError())
            exitProcess(1)
        }
    }

}

abstract class ObjectState

class Pending : ObjectState() {
    val pendingReads = mutableListOf<Long>()
    val pendingReplications = mutableListOf<Host>()
}

class Present : ObjectState()



