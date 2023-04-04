package storage

import org.apache.logging.log4j.LogManager

class InMemoryWrapper : StorageWrapper {

    companion object {
        private val logger = LogManager.getLogger()
    }

    private val data = mutableMapOf<String, String>()

    override fun initialize() {
        logger.info("In memory storage initialized")
    }

    override fun put(key: String, value: String) {
        TODO("Not yet implemented")
    }

    override fun get(key: String): String? {
        TODO("Not yet implemented")
    }

    override fun delete(key: String) {
        TODO("Not yet implemented")
    }
}