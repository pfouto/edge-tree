package storage

import Config
import ipc.ActivateNotification
import ipc.DeactivateNotification
import ipc.OpReply
import ipc.OpRequest
import org.apache.logging.log4j.LogManager
import proxy.ClientProxy
import proxy.utils.MigrationOperation
import proxy.utils.ReadOperation
import proxy.utils.WriteOperation
import pt.unl.fct.di.novasys.babel.core.GenericProtocol
import tree.utils.HybridTimestamp
import java.net.Inet4Address
import java.util.*

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
    private var storageWrapper: StorageWrapper? = null

    init {
        subscribeNotification(DeactivateNotification.ID) { _: DeactivateNotification, _ -> onDeactivate() }
        subscribeNotification(ActivateNotification.ID) { not: ActivateNotification, _ -> onActivate(not) }

        registerRequestHandler(OpRequest.ID) { req: OpRequest, _ -> onLocalOpRequest(req) }
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

    private fun onLocalOpRequest(req: OpRequest) {
        when (req.op) {
            is WriteOperation -> {
                val hlc = currentHlc()
                storageWrapper!!.put(req.op.partition, req.op.key, DataObject(req.op.value, hlc, lww))
                sendReply(OpReply(req.id, hlc, null), ClientProxy.ID)

                //If not local, send request to replicate to tree without adding to pending
                if(localData[req.op.partition]?.get(req.op.key) == null){
                    localData.computeIfAbsent(req.op.partition) { mutableMapOf() }[req.op.key] = Pending()
                    //Send req to tree
                }

                //Send op to tree (with persistency)

                //if persistency, also add to pending (different from read and migration pending)
            }

            is ReadOperation -> {
                //Check if available locally
                //if yes, execute

                //if no, add to pending and send request to tree

            }

            is MigrationOperation -> {
                //Add to pending (different from read pending)
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
}

abstract class ObjectState
class Pending : ObjectState() {
    val pendingOps = mutableListOf<OpRequest>()
}
class Present : ObjectState() {
    val pendingOps = mutableListOf<OpRequest>()
}