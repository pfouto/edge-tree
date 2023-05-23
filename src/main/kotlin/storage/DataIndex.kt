package storage

class DataIndex {

    private val partitions: MutableMap<String, Partition> = mutableMapOf()

    fun containsFullPartition(partitionName: String): Boolean {
        return partitions[partitionName] is FullPartition
    }

    fun partitionIterator(): Iterator<Partition> {
        return partitions.values.iterator()
    }

    fun containsObject(id: ObjectIdentifier): Boolean {
        partitions[id.partition]?.let { partition ->
            if (partition is FullPartition) return true
            else if (partition is PartialPartition) return partition.keys.contains(id.key)
        }
        return false
    }

    fun addFullPartition(partitionName: String): Boolean {
        if (partitions[partitionName] is FullPartition)
            return false
        partitions[partitionName] = FullPartition(partitionName)
        return true
    }

    fun addObject(objId: ObjectIdentifier) {
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

}