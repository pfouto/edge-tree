package storage

import org.apache.cassandra.config.Config
import org.apache.cassandra.config.DatabaseDescriptor
import org.apache.cassandra.config.YamlConfigurationLoader
import org.apache.cassandra.cql3.QueryProcessor
import org.apache.cassandra.db.ConsistencyLevel
import org.apache.cassandra.io.util.File
import org.apache.cassandra.service.EmbeddedCassandraService
import org.apache.logging.log4j.LogManager
import java.net.URL
import java.util.*

class CassandraWrapper() : StorageWrapper{

    companion object {
        private val logger = LogManager.getLogger()
    }

    private var cassandra: EmbeddedCassandraService? = null

    override fun initialize() {

        logger.info("Instantiating cassandra")

        val config = YamlConfigurationLoader().loadConfig(URL("file:./cassandra.yaml"))
        Config.setOverrideLoadConfig { config }


        System.setProperty("cassandra.storagedir", "/tmp/cassandra")

        //cleanupDirectory("/tmp/cassandra/")


        DatabaseDescriptor.daemonInitialization()
        mkdirs()
        cleanup()
        cassandra = EmbeddedCassandraService()
        cassandra!!.start()

        logger.info("Creating cassandra schema")

        createSchema()
        //daemon.destroyClientTransports()

        logger.info("Cassandra instantiated")

    }

    override fun put(objId: ObjectIdentifier, objData: ObjectData): ObjectData {
        TODO("Not yet implemented")
    }

    override fun get(objId: ObjectIdentifier): ObjectData? {
        TODO("Not yet implemented")
    }

    override fun delete(objId: ObjectIdentifier): ObjectData? {
        TODO("Not yet implemented")
    }

    override fun cleanUp() {
        TODO("Not yet implemented")
    }

    override fun getFullPartitionData(partition: String): List<Pair<String, ObjectData>> {
        TODO("Not yet implemented")
    }


    private fun createSchema() {
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

    private fun mkdirs() {
        DatabaseDescriptor.createAllDirectories()
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

    private fun cleanupSavedCaches() {
        cleanupDirectory(DatabaseDescriptor.getSavedCachesLocation())
    }


}