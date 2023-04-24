package storage

import Config
import ipc.*
import org.apache.logging.log4j.LogManager
import proxy.ClientProxy
import proxy.utils.MigrationOperation
import proxy.utils.ReadOperation
import proxy.utils.WriteOperation
import pt.unl.fct.di.novasys.babel.core.GenericProtocol
import tree.Tree
import tree.TreeProto
import tree.utils.HybridTimestamp
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

    private val lww = address.hashCode()

    private val localData = mutableMapOf<String, MutableMap<String, ObjectState>>()
    //Sorted list of pending migrations (sorted by timestamp)
    //private val pendingMigrations =

    private val pendingPersistence = mutableMapOf<Int, MutableList<Long>>()

    private var storageWrapper: StorageWrapper? = null

    init {
        subscribeNotification(DeactivateNotification.ID) { _: DeactivateNotification, _ -> onDeactivate() }
        subscribeNotification(ActivateNotification.ID) { not: ActivateNotification, _ -> onActivate(not) }

        registerRequestHandler(OpRequest.ID) { req: OpRequest, _ -> onLocalOpRequest(req) }
        registerReplyHandler(PropagateWriteReply.ID) { rep: PropagateWriteReply, _ -> onRemoteWrite(rep.write) }
        registerReplyHandler(ReplicationReply.ID) { rep: ReplicationReply, _ -> onReplicationReply(rep) }
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

    private fun onReplicationReply(rep: ReplicationReply) {
        val newValue = if (rep.obj != null)
            storageWrapper!!.put(rep.partition, rep.key, rep.obj)
        else
            storageWrapper!!.get(rep.partition, rep.key)

        val partition = localData[rep.partition]!!
        val pending = partition[rep.key]!! as Pending


        for (id in pending.pendingReads) {
            if (newValue == null) sendReply(OpReply(id, null, null), ClientProxy.ID)
            else sendReply(OpReply(id, newValue.hlc, newValue.value), ClientProxy.ID)
        }

        partition[rep.key] = Present()
    }

    private fun onRemoteWrite(write: RemoteWrite) {
        assertOrExit(
            localData[write.partition]?.get(write.key) != null,
            "Received write for non-existent object ${write.partition} ${write.key}"
        )
        storageWrapper!!.put(write.partition, write.key, write.dataObject)
    }

    private fun onLocalOpRequest(req: OpRequest) {
        when (req.op) {
            is WriteOperation -> {
                val hlc = currentHlc()
                val newObj = DataObject(req.op.value, hlc, lww)
                storageWrapper!!.put(req.op.partition, req.op.key, newObj)
                sendReply(OpReply(req.id, hlc, null), ClientProxy.ID)

                //Check if available locally, if not, send replication request to tree
                val partition = localData.computeIfAbsent(req.op.partition) { mutableMapOf() }
                partition.computeIfAbsent(req.op.key) {
                    sendRequest(ReplicationRequest(req.op.partition, req.op.key), TreeProto.ID)
                    Pending(System.currentTimeMillis())
                }

                //Ask tree to propagate write
                sendRequest(
                    PropagateWriteRequest(req.id, RemoteWrite(req.op.partition, req.op.key, newObj)),
                    TreeProto.ID
                )

                //if persistency, also add to pending (different from read and migration pending)
                if (req.op.persistence > 0) {
                    if (req.op.persistence == 1.toShort())
                        sendReply(ClientWritePersistent(req.id), ClientProxy.ID)
                    else
                        pendingPersistence.computeIfAbsent(req.op.persistence.toInt()) { mutableListOf() }.add(req.id)
                }
            }

            is ReadOperation -> {
                //Check if available locally, if not, send replication request to tree
                val partition = localData.computeIfAbsent(req.op.partition) { mutableMapOf() }
                val objState = partition.computeIfAbsent(req.op.key) {
                    sendRequest(ReplicationRequest(req.op.partition, req.op.key), TreeProto.ID)
                    Pending(System.currentTimeMillis())
                }

                //Execute if present, or add to pending if not
                when (objState) {
                    is Present -> {
                        when (val data = storageWrapper!!.get(req.op.partition, req.op.key)) {
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

class Pending(val timestamp: Long) : ObjectState() {
    val pendingReads = mutableListOf<Long>()
}

class Present : ObjectState()


