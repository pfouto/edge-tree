package tree.utils

import org.apache.logging.log4j.LogManager
import pt.unl.fct.di.novasys.network.data.Host

class ChildState(val child: Host) {
    enum class State {
        SYNC, READY
    }

    companion object {
        private val logger = LogManager.getLogger()
    }

    var childStableTime: HybridTimestamp = HybridTimestamp()
    var state: State = State.SYNC

}
