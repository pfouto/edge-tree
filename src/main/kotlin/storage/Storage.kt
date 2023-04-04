package storage

import Config
import ipc.ActivateNotification
import ipc.DeactivateNotification
import org.apache.logging.log4j.LogManager
import pt.unl.fct.di.novasys.babel.core.GenericProtocol
import java.net.Inet4Address
import java.util.*

class Storage(address: Inet4Address, private val config: Config) : GenericProtocol(NAME, ID) {

    companion object {
        const val NAME = "Storage"
        const val ID: Short = 500

        const val CASSANDRA_TYPE = "cassandra"
        const val IN_MEMORY_TYPE = "in_memory"

        private val logger = LogManager.getLogger()
    }

    private var storageWrapper: StorageWrapper? = null

    init {
        subscribeNotification(DeactivateNotification.ID) { _: DeactivateNotification, _ -> onDeactivate() }
        subscribeNotification(ActivateNotification.ID) { not: ActivateNotification, _ -> onActivate(not) }

    }

    override fun init(props: Properties) {

    }

    private fun onActivate(notification: ActivateNotification) {
        storageWrapper = if(notification.contact != null) //datacenter
            if(config.dc_storage_type == CASSANDRA_TYPE) CassandraWrapper() else InMemoryWrapper()
        else // node
            if(config.node_storage_type == CASSANDRA_TYPE) CassandraWrapper() else InMemoryWrapper()
        storageWrapper!!.initialize()
    }

    private fun onDeactivate(){
        logger.info("Storage Deactivating (nothing)")
    }
}