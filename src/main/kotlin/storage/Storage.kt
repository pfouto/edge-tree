package storage

import Config
import ipc.ActivateNotification
import ipc.DeactivateNotification
import org.apache.logging.log4j.LogManager
import pt.unl.fct.di.novasys.babel.core.GenericProtocol
import java.net.Inet4Address
import java.util.*

class Storage(address: Inet4Address, config: Config) : GenericProtocol(NAME, ID) {

    companion object {
        const val NAME = "Storage"
        const val ID: Short = 500

        private val logger = LogManager.getLogger()
    }

    private val storageProxy: CassandraWrapper = CassandraWrapper(address)

    init {
        subscribeNotification(DeactivateNotification.ID) { _: DeactivateNotification, _ -> onDeactivate() }
        subscribeNotification(ActivateNotification.ID) { not: ActivateNotification, _ -> onActivate(not) }


    }

    override fun init(props: Properties) {

    }

    private fun onActivate(notification: ActivateNotification) {
        logger.info("Instantiating cassandra")
        storageProxy.initialize()
        logger.info("Creating schema")
        storageProxy.createSchema()
    }

    private fun onDeactivate(){
        logger.info("Storage Deactivating")
    }




}