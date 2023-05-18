package tree.utils

import pt.unl.fct.di.novasys.network.data.Host
import storage.ObjectIdentifier

abstract class ChildState(val child: Host)

class ChildSync(child: Host) : ChildState(child)

class ChildReady(
    child: Host,
    val objects: MutableMap<ObjectIdentifier, ChildObjectState>,
    var childStableTime: HybridTimestamp = HybridTimestamp(),
) : ChildState(child)

enum class ChildObjectState {
    PENDING, READY
}



