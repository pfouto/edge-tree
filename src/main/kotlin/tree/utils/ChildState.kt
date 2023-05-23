package tree.utils

import pt.unl.fct.di.novasys.network.data.Host
import storage.DataIndex
import storage.ObjectIdentifier
import storage.RemoteWrite

abstract class ChildState(val child: Host)

class ChildConnected(child: Host) : ChildState(child)

class ChildSync(child: Host, val objects: DataIndex, var childStableTime: HybridTimestamp = HybridTimestamp())
    : ChildState(child)

class ChildReady(
    child: Host,
    val objects: DataIndex,
    val pendingObjects: Map<ObjectIdentifier, MutableList<RemoteWrite>> = mutableMapOf(),
    val pendingFullPartitions: Map<String, MutableList<RemoteWrite>> = mutableMapOf(),
    var childStableTime: HybridTimestamp = HybridTimestamp(),
) : ChildState(child)