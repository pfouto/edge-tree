package tree

import org.apache.logging.log4j.LogManager
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage
import pt.unl.fct.di.novasys.network.data.Host
import tree.utils.HybridTimestamp
import tree.utils.messaging.SyncRequest
import tree.utils.messaging.SyncResponse
import tree.utils.messaging.UpstreamMetadata
import java.util.function.Consumer

class ChildState(val child: Host, val proxy: Consumer<ProtoMessage>) {
    enum class State {
        SYNC, READY
    }
    companion object {
        private val logger = LogManager.getLogger()
    }

    var childStableTime: HybridTimestamp
    var state: State

    init{
        state = State.SYNC
        childStableTime = HybridTimestamp()
    }

}
