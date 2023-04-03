package storage

import org.apache.cassandra.config.Config
import org.apache.cassandra.config.DatabaseDescriptor
import org.apache.cassandra.config.YamlConfigurationLoader
import org.apache.cassandra.cql3.QueryProcessor
import org.apache.cassandra.db.ConsistencyLevel
import org.apache.cassandra.io.util.File
import org.apache.cassandra.service.CassandraDaemon
import org.apache.cassandra.service.EmbeddedCassandraService
import org.apache.logging.log4j.LogManager
import java.net.Inet4Address
import java.net.URL
import java.util.*

class CassandraWrapper(address: Inet4Address) {

    companion object {
        private val logger = LogManager.getLogger()
    }

    private val cassandraConfigMap: Map<String, Any>

    init{
        cassandraConfigMap = mapOf<String, Any>(
            "cluster_name" to "Test Cluster",
            "listen_address" to address.hostAddress,
        )

    }

    private val daemon: CassandraDaemon = CassandraDaemon(true)

    private var cassandra: EmbeddedCassandraService? = null

    fun initialize() {

        val config = YamlConfigurationLoader().loadConfig(URL("file:./cassandra.yaml"))
        Config.setOverrideLoadConfig { config }


        System.setProperty("cassandra.storagedir", "/tmp/cassandra")

        DatabaseDescriptor.daemonInitialization()
        mkdirs()
        //cleanup()
        cassandra = EmbeddedCassandraService()
        cassandra!!.start()

        //daemon.destroyClientTransports()
    }

    fun createSchema() {
        //TODO create keyspace and tables

        logger.info(
            QueryProcessor.process(
                "CREATE KEYSPACE IF NOT EXISTS tree WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};",
                ConsistencyLevel.ONE
            )
        )
        logger.info(
            QueryProcessor.process(
                "CREATE TABLE IF NOT EXISTS tree.data (key text PRIMARY KEY, value text);",
                ConsistencyLevel.ONE
            )
        )
    }

    fun terminate() {
        if (cassandra != null) {
            cassandra!!.stop()
            logger.info("Done")
        }
    }

    private fun cleanup() {
        // clean up commitlog
        cleanupDirectory(DatabaseDescriptor.getCommitLogLocation())
        val cdcDir = DatabaseDescriptor.getCDCLogLocation()
        if (cdcDir != null) cleanupDirectory(cdcDir)
        cleanupDirectory(DatabaseDescriptor.getHintsDirectory())
        cleanupSavedCaches()

        // clean up data directory which are stored as data directory/keyspace/data files
        for (dirName in DatabaseDescriptor.getAllDataFileLocations()) {
            cleanupDirectory(dirName)
        }
    }

    private fun cleanupDirectory(dirName: String?) {
        if (dirName != null) cleanupDirectory(File(dirName))
    }

    private fun cleanupDirectory(directory: File) {
        if (directory.exists()) {
            Arrays.stream(directory.tryList()).forEach { obj: File -> obj.deleteRecursive() }
        }
    }

    private fun mkdirs() {
        DatabaseDescriptor.createAllDirectories()
    }

    private fun cleanupSavedCaches() {
        cleanupDirectory(DatabaseDescriptor.getSavedCachesLocation())
    }


}