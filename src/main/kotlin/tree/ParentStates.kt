package tree

import pt.unl.fct.di.novasys.network.data.Host
import tree.utils.HybridTimestamp

abstract class ParentState

class ParentNone : ParentState() {
    override fun toString(): String {
        return "ParentNone"
    }
}

abstract class ParentSome(val parent: Host, var backups: MutableList<Host>) : ParentState()

class ParentConnecting(parent: Host, backups: MutableList<Host>, var retries: Int = 0) : ParentSome(parent, backups) {
    override fun toString(): String {
        return "ParentConnecting($parent)"
    }
}

class ParentSync(parent: Host, val proxy: Tree.MessageSenderOut, backups: MutableList<Host>) : ParentSome(parent, backups) {
    override fun toString(): String {
        return "ParentSync($parent)"
    }
}

class ParentReady(
    parent: Host,
    val proxy: Tree.MessageSenderOut,
    var predecessors: List<Pair<Host, HybridTimestamp>>,
    backups: MutableList<Host>
) :
    ParentSome(parent, backups) {
    override fun toString(): String {
        return "ParentReady($parent, $predecessors)"
    }
}
