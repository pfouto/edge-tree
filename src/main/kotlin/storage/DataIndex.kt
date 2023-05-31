package storage

import tree.messaging.up.SyncRequest

open class DataIndex {

    private val partitions: MutableMap<String, Partition> = mutableMapOf()

    companion object {
        fun fromSyncRequest(syncRequest: SyncRequest): DataIndex {
            val index = DataIndex()
            syncRequest.fullPartitions.forEach { (partition, _) ->
                index.partitions[partition] = FullPartition(partition)
            }
            syncRequest.partialPartitions.forEach { (partition, objects) ->
                index.partitions[partition] = PartialPartition(partition, objects.keys.toMutableSet())
            }
            return index
        }
    }

    open fun containsFullPartition(partitionName: String): Boolean {
        return partitions[partitionName] is FullPartition
    }

    open fun partitionIterator(): Iterator<Partition> {
        return partitions.values.iterator()
    }

    open fun containsObject(id: ObjectIdentifier): Boolean {
        partitions[id.partition]?.let { partition ->
            if (partition is FullPartition) return true
            else if (partition is PartialPartition) return partition.keys.contains(id.key)
        }
        return false
    }

    open fun addFullPartition(partitionName: String): Boolean {
        if (partitions[partitionName] is FullPartition)
            return false
        partitions[partitionName] = FullPartition(partitionName)
        return true
    }

    open fun addObject(objId: ObjectIdentifier) {
        when (val partition = partitions[objId.partition]) {
            is FullPartition -> return
            is PartialPartition -> partition.keys.add(objId.key)
            null -> partitions[objId.partition] = PartialPartition.single(objId.partition, objId.key)
        }
    }

    fun clear() {
        partitions.clear()
    }

    abstract class Partition(val name: String)

    class FullPartition(name: String) : Partition(name)

    class PartialPartition(name: String, val keys: MutableSet<String> = mutableSetOf()) :
        Partition(name) {

        fun keyIterator(): Iterator<String> {
            return keys.iterator()
        }

        companion object {
            fun single(name: String, key: String): PartialPartition {
                val partition = PartialPartition(name)
                partition.keys.add(key)
                return partition
            }
        }

    }

    class DCDataIndex : DataIndex() {
        override fun containsFullPartition(partitionName: String): Boolean {
            return true
        }

        override fun addFullPartition(partitionName: String): Boolean {
            throw UnsupportedOperationException("Cannot add full partitions to DCDataIndex")
        }

        override fun addObject(objId: ObjectIdentifier) {
            throw UnsupportedOperationException("Cannot add objects to DCDataIndex")
        }

        override fun partitionIterator(): Iterator<Partition> {
            throw UnsupportedOperationException("Cannot iterate over partitions in DCDataIndex")
        }

        override fun containsObject(id: ObjectIdentifier): Boolean {
            return true
        }


    }
}