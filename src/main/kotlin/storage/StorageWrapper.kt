package storage

import tree.utils.HybridTimestamp

interface StorageWrapper {
    fun initialize()
    fun put(partitionKey: String, key: String, dataObject: DataObject)
    fun get(partitionKey: String, key: String): DataObject?
    fun delete(partitionKey: String, key: String): DataObject?
    fun cleanUp()

}

data class DataObject(val value: ByteArray, val hlc: HybridTimestamp, val lww: Int) {
    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false

        other as DataObject

        if (!value.contentEquals(other.value)) return false
        if (hlc != other.hlc) return false
        return lww == other.lww
    }

    override fun hashCode(): Int {
        var result = value.contentHashCode()
        result = 31 * result + hlc.hashCode()
        result = 31 * result + lww
        return result
    }
}
