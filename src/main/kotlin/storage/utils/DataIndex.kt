package storage.utils

import pt.unl.fct.di.novasys.network.data.Host
import storage.ObjectIdentifier
import tree.utils.ChildReady

open class DataIndex {

    private val partitions: MutableMap<String, Partition> = mutableMapOf()

    override fun toString(): String {
        return "DataIndex(${partitions.toSortedMap().values})"
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

    open fun updateTimestamp(id: ObjectIdentifier) {
        partitions[id.partition]!!.let { partition ->
            if (partition is FullPartition) partition.lastAccess = System.currentTimeMillis()
            else if (partition is PartialPartition) partition.keys[id.key] = System.currentTimeMillis()
        }
    }


    open fun addFullPartition(partitionName: String): Boolean {
        if (partitions[partitionName] is FullPartition)
            return false
        partitions[partitionName] = FullPartition(partitionName, System.currentTimeMillis())
        return true
    }

    open fun addObject(objId: ObjectIdentifier) {
        when (val partition = partitions[objId.partition]) {
            is FullPartition -> return
            is PartialPartition -> partition.keys[objId.key] = System.currentTimeMillis()
            null -> partitions[objId.partition] = PartialPartition.single(objId.partition, objId.key)
        }
    }

    fun clear() {
        partitions.clear()
    }

    open fun garbageCollect(
        now: Long,
        threshold: Long,
        childData: MutableMap<Host, ChildReady>,
    ): Pair<Set<ObjectIdentifier>, Set<String>> {
        val deletedObjects = mutableSetOf<ObjectIdentifier>()
        val deletedPartitions = mutableSetOf<String>()

        val partitionIterator = partitions.iterator()
        while (partitionIterator.hasNext()) {
            val (partitionName, partition) = partitionIterator.next()
            if (partition is FullPartition && now - partition.lastAccess > threshold
                && childData.values.none { it.containsPartition(partitionName) }
            ) {
                deletedPartitions.add(partitionName)
                partitionIterator.remove()
            } else if (partition is PartialPartition) {
                val keyIterator = partition.keys.iterator()
                while (keyIterator.hasNext()) {
                    val (key, lastAccess) = keyIterator.next()
                    val objId = ObjectIdentifier(partitionName, key)
                    if (now - lastAccess > threshold && childData.values.none { it.containsObject(objId) }) {
                        deletedObjects.add(ObjectIdentifier(partitionName, key))
                        keyIterator.remove()
                    }
                }
            }
        }
        return Pair(deletedObjects, deletedPartitions)
    }

    fun nObjects(): Int {
        return partitions.values.sumOf {
            when (it) {
                is FullPartition -> 0
                is PartialPartition -> it.keys.size
                else -> 0
            }
        }
    }

    fun allObjects(): String {
        return partitions.values.joinToString(separator = ",") {
            when (it) {
                is FullPartition -> ""
                is PartialPartition -> it.keys.keys.joinToString(separator = ","){ key -> "${it.name}:$key" }
                else -> ""
            }
        }
    }

    abstract class Partition(val name: String)

    class FullPartition(name: String, var lastAccess: Long) : Partition(name) {
        override fun toString(): String {
            return "Full '$name'"
        }
    }

    class PartialPartition(name: String, val keys: MutableMap<String, Long> = mutableMapOf()) :
        Partition(name) {

        override fun toString(): String {
            return "Partial '$name' ${keys.size}"
        }

        fun keyIterator(): Iterator<String> {
            return keys.keys.iterator()
        }

        companion object {
            fun single(name: String, key: String): PartialPartition {
                val partition = PartialPartition(name)
                partition.keys[key] = System.currentTimeMillis()
                return partition
            }
        }

    }

    class DCDataIndex : DataIndex() {

        override fun updateTimestamp(id: ObjectIdentifier) {
            return
        }

        override fun toString(): String {
            return "DCDataIndex"
        }

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

        override fun garbageCollect(
            now: Long,
            threshold: Long,
            childData: MutableMap<Host, ChildReady>,
        ): Pair<Set<ObjectIdentifier>, Set<String>> {
            throw UnsupportedOperationException("Cannot gc in DCDataIndex")
        }

        override fun containsObject(id: ObjectIdentifier): Boolean {
            return true
        }


    }
}