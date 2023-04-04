package storage

interface StorageWrapper {
    fun initialize()
    fun put(key: String, value: String)
    fun get(key: String): String?
    fun delete(key: String)
}