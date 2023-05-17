package tree.utils

import pt.unl.fct.di.novasys.network.data.Host

abstract class State

/* Types of states:
    Inactive
    Datacenter
    Node
        ParentConnecting
        ConnectedNode
            ParentSync
            ParentReady
 */

class Inactive : State() {
    override fun toString(): String {
        return "INACTIVE"
    }
}

class Datacenter : State() {
    override fun toString(): String {
        return "DATACENTER"
    }
}

abstract class Node(val parent: Host, val grandparents: List<Host>, val dataRequests: MutableSet<Pair<String, String>>) :
    State()

class ParentConnecting(
    parent: Host,
    grandparents: List<Host>,
    requests: MutableSet<Pair<String, String>>,
    val retries: Int = 1,
) : Node(parent, grandparents, requests) {
    override fun toString(): String {
        return "PARENT_CONNECTING $parent $grandparents $retries"
    }
}

abstract class ConnectedNode(parent: Host, grandparents: List<Host>, requests: MutableSet<Pair<String, String>>) :
    Node(parent, grandparents, requests)

class ParentSync(parent: Host, grandparents: List<Host>, requests: MutableSet<Pair<String, String>>) :
    ConnectedNode(parent, grandparents, requests) {

    companion object {
        fun fromConnecting(connecting: ParentConnecting): ParentSync {
            return ParentSync(connecting.parent, connecting.grandparents, connecting.dataRequests)
        }
    }

    override fun toString(): String {
        return "PARENT_SYNC $parent $grandparents"
    }
}

class ParentReady(
    parent: Host,
    grandparents: List<Host>,
    val metadata: List<Metadata>,
    requests: MutableSet<Pair<String, String>>,
) :
    ConnectedNode(parent, grandparents, requests) {
    override fun toString(): String {
        return "PARENT_READY $parent ${grandparents.joinToString(",", prefix = "[", postfix = "]")}"
    }
}

data class Metadata(var timestamp: HybridTimestamp) {
    override fun toString(): String {
        return "$timestamp"
    }
}
