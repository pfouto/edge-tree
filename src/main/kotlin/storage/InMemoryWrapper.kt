package storage

import org.apache.logging.log4j.LogManager
import tree.utils.HybridTimestamp
import java.net.Inet4Address

class InMemoryWrapper : StorageWrapper {

    companion object {
        private val logger = LogManager.getLogger()
    }

    private val data = mutableMapOf<String, MutableMap<String, DataObject>>()

    override fun initialize() {
        logger.info("In memory storage initialized")
    }

    override fun put(partitionKey: String, key: String, dataObject: DataObject): DataObject {
        val partition = data.computeIfAbsent(partitionKey) { mutableMapOf() }
        return partition.merge(key, dataObject) { old, new -> if (new.hlc.isAfter(old.hlc)) new else old }!!
    }

    override fun get(partitionKey: String, key: String): DataObject? {
        return data[partitionKey]?.get(key)
    }

    override fun delete(partitionKey: String, key: String): DataObject? {
        return data[partitionKey]?.remove(key)
    }

    override fun cleanUp() {
        data.forEach { (_, v) -> v.clear() }
        data.clear()
    }
}