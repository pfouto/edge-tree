package tree.utils

import pt.unl.fct.di.novasys.network.data.Host
import storage.DataIndex
import storage.ObjectIdentifier
import storage.RemoteWrite

abstract class ChildState(val child: Host)

class ChildConnected(child: Host) : ChildState(child)

abstract class ChildMeta(
    child: Host,
    val objects: DataIndex,
    var childStableTime: HybridTimestamp = HybridTimestamp(),
) : ChildState(child)

class ChildSync(child: Host, objects: DataIndex, childStableTime: HybridTimestamp = HybridTimestamp()) :
    ChildMeta(child, objects, childStableTime)

class ChildReady(
    child: Host,
    objects: DataIndex,
    childStableTime: HybridTimestamp = HybridTimestamp(),
    val pendingObjects: Map<ObjectIdentifier, MutableList<RemoteWrite>> = mutableMapOf(),
    val pendingFullPartitions: Map<String, MutableList<RemoteWrite>> = mutableMapOf(),
) : ChildMeta(child, objects, childStableTime)