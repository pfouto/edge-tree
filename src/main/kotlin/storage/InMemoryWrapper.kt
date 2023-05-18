package storage

import org.apache.logging.log4j.LogManager

class InMemoryWrapper : StorageWrapper {

    companion object {
        private val logger = LogManager.getLogger()
    }

    private val data = mutableMapOf<String, MutableMap<String, DataObject>>()

    override fun initialize() {
        logger.info("In memory storage initialized")
    }

    override fun put(objId: ObjectIdentifier, objData: DataObject): DataObject {
        val partition = data.computeIfAbsent(objId.partition) { mutableMapOf() }
        return partition.merge(objId.key, objData) { old, new -> if (new.hlc.isAfter(old.hlc)) new else old }!!
    }

    override fun get(objId: ObjectIdentifier): DataObject? {
        return data[objId.partition]?.get(objId.key)
    }

    override fun delete(objId: ObjectIdentifier): DataObject? {
        return data[objId.partition]?.remove(objId.key)
    }

    override fun cleanUp() {
        data.forEach { (_, v) -> v.clear() }
        data.clear()
    }
}