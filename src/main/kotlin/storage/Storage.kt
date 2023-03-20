package storage

import ipc.ActivateNotification
import org.apache.logging.log4j.LogManager
import pt.unl.fct.di.novasys.babel.core.GenericProtocol
import java.net.Inet4Address
import java.util.*

class Storage(address: Inet4Address, props: Properties) : GenericProtocol(NAME, ID) {

    companion object {
        const val NAME = "Storage"
        const val ID: Short = 500

        private val logger = LogManager.getLogger()
    }

    private val cassandraConfigMap: Map<String, Any>

    private val storageProxy: CassandraWrapper = CassandraWrapper()

    init {
        subscribeNotification(ActivateNotification.ID) { not: ActivateNotification, _ -> activate(not) }

        cassandraConfigMap = mapOf<String, Any>(
            "cluster_name" to "Test Cluster",
            "listen_address" to address.hostAddress,
        )

    }

    override fun init(props: Properties) {

    }

    private fun activate(notification: ActivateNotification) {
        logger.info("Instantiating cassandra")
        storageProxy.initialize()
        logger.info("Creating schema")
        storageProxy.createSchema()

        logger.info("Block")
    }




}