package tree.utils

import pt.unl.fct.di.novasys.network.data.Host
import storage.ObjectIdentifier
import storage.RemoteWrite

abstract class ChildState(val child: Host)

class ChildSync(child: Host) : ChildState(child)

class ChildReady(
    child: Host,
    //TODO DataIndex
    val objects: MutableMap<ObjectIdentifier, ChildObjectState>,
    //TODO pending list with operation queue for pending objects
    val pendingObjects: Map<ObjectIdentifier, Pair<MutableList<Long>, MutableList<Host>>>,
    val pendingFullPartitions: Map<String, MutableList<RemoteWrite>>,

    var childStableTime: HybridTimestamp = HybridTimestamp(),
) : ChildState(child)

enum class ChildObjectState {
    PENDING, READY
}



