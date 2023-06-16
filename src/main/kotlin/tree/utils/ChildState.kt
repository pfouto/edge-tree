package tree.utils

import pt.unl.fct.di.novasys.network.data.Host
import storage.utils.ChildDataIndex
import storage.ObjectIdentifier
import storage.RemoteWrite
import java.util.*

abstract class ChildState(val child: Host)

class ChildConnected(child: Host) : ChildState(child)

abstract class ChildMeta(
    child: Host,
    val objects: ChildDataIndex,
    var childStableTime: HybridTimestamp = HybridTimestamp(),
) : ChildState(child)

class ChildSync(
    child: Host,
    objects: ChildDataIndex,
    val pendingWrites: MutableList<Pair<WriteID, RemoteWrite>> = mutableListOf(),
    childStableTime: HybridTimestamp = HybridTimestamp(),
) : ChildMeta(child, objects, childStableTime)

class ChildReady(
    child: Host,
    objects: ChildDataIndex,
    childStableTime: HybridTimestamp = HybridTimestamp(),
    val pendingObjects: MutableMap<ObjectIdentifier, MutableList<Pair<WriteID, RemoteWrite>>> = mutableMapOf(),
    val pendingFullPartitions: MutableMap<String, MutableList<Pair<WriteID, RemoteWrite>>> = mutableMapOf(),
    val persistenceMapper: TreeMap<Int, Int> = TreeMap(),
    var highestPersistenceIdSeen: Int = 0
) : ChildMeta(child, objects, childStableTime)
