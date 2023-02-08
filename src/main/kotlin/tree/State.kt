package tree

import pt.unl.fct.di.novasys.network.data.Host
import tree.utils.HybridTimestamp

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

abstract class Node(val parent: Host, val grandparents: List<Host>) : State()

class ParentConnecting(parent: Host, grandparents: List<Host>, val retries: Int = 1) : Node(parent, grandparents) {
    override fun toString(): String {
        return "PARENT_CONNECTING $parent $grandparents $retries"
    }
}

abstract class ConnectedNode(parent: Host, val proxy: Tree.ParentProxy, grandparents: List<Host>) :
    Node(parent, grandparents)

class ParentSync(parent: Host, proxy: Tree.ParentProxy, grandparents: List<Host>) :
    ConnectedNode(parent, proxy, grandparents) {
    override fun toString(): String {
        return "PARENT_SYNC $parent $grandparents"
    }
}

class ParentReady(
    parent: Host,
    proxy: Tree.ParentProxy,
    grandparents: List<Host>,

    val metadata: List<Metadata>,
) :
    ConnectedNode(parent, proxy, grandparents) {
    override fun toString(): String {

        return "PARENT_READY $parent ${grandparents.joinToString(",", prefix = "[", postfix = "]")}"
    }
}

data class Metadata(var timestamp: HybridTimestamp) {
    override fun toString(): String {
        return "$timestamp"
    }
}
