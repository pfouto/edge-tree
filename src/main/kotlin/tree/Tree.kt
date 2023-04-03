package tree

import Config
import getTimeMillis
import ipc.ActivateNotification
import ipc.DeactivateNotification
import org.apache.logging.log4j.LogManager
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage
import pt.unl.fct.di.novasys.channel.tcp.TCPChannel
import pt.unl.fct.di.novasys.network.data.Host
import tree.utils.*
import tree.messaging.down.Downstream
import tree.messaging.down.Reconfiguration
import tree.messaging.down.Reject
import tree.messaging.down.SyncResponse
import tree.messaging.up.SyncRequest
import tree.messaging.up.Upstream
import java.net.Inet4Address
import kotlin.system.exitProcess

class Tree(address: Inet4Address, config: Config) : TreeProto(address, config) {

    companion object {
        private val logger = LogManager.getLogger()
        const val MAX_RECONNECT_RETRIES = 3
    }

    //Self
    private var timestamp: HybridTimestamp = HybridTimestamp(getTimeMillis(), 0)

    //Children
    private var stableTimestamp: HybridTimestamp = HybridTimestamp(0, 0)
    private val children: MutableMap<Host, ChildState> = mutableMapOf()

    //Parent
    private var state: State = Inactive()
        private set(value) {
            field = value
            logger.info("TREE-STATE $state")
        }

    override fun onActivate(notification: ActivateNotification) {
        logger.info("$notification received")
        if (state !is Inactive) {
            logger.warn("Already active, ignoring")
            return
        }

        if (notification.contact == null) {
            state = Datacenter()
        } else
            newParent(Host(notification.contact, PORT))
    }

    override fun onDeactivate() {

    }

    /* ------------------------------- PARENT HANDLERS ------------------------------------------- */
    private fun newParent(parent: Host, backups: List<Host> = mutableListOf()) {
        state = ParentConnecting(parent, backups)
        openConnection(parent)
    }

    override fun parentConnected(host: Host) {
        assertOrExit(state is ParentConnecting, "ConnectionUp while not connecting: $state")
        parentMatchesOrExit(host)

        state = ParentSync(host, (state as ParentConnecting).grandparents)
        updateTsAndStableTs()
        sendMessage(SyncRequest(Upstream(stableTimestamp), mutableListOf()), host, TCPChannel.CONNECTION_OUT)
    }

    override fun onParentSyncResponse(host: Host, msg: SyncResponse) {
        assertOrExit(state is ParentSync, "Sync resp while not sync $state")
        parentMatchesOrExit(host)

        state = ParentReady(host, emptyList(), emptyList())
        onReconfiguration(host, msg.reconfiguration)
    }

    override fun onReconfiguration(host: Host, reconfiguration: Reconfiguration) {
        assertOrExit(state is ParentReady, "Reconfiguration while not ready $state")
        parentMatchesOrExit(host)

        val metadata = mutableListOf<Metadata>()
        for (i in 0 until reconfiguration.grandparents.size + 1)
            metadata.add(Metadata(HybridTimestamp()))

        state = ParentReady(host, reconfiguration.grandparents, metadata)

        onDownstream(host, reconfiguration.downstream)

        val reconfigMsg = buildReconfigurationMessage()
        for (childState in children.values)
            sendMessage(reconfigMsg, childState.child, TCPChannel.CONNECTION_IN)
    }

    override fun onDownstream(host: Host, msg: Downstream) {
        assertOrExit(state is ParentReady, "DownstreamMetadata while not ready $state")
        parentMatchesOrExit(host)

        val ready = state as ParentReady
        assertOrExit(msg.timestamps.size == ready.grandparents.size + 1, "Wrong number of timestamps")
        assertOrExit(msg.timestamps.size == ready.metadata.size, "Wrong number of timestamps")

        for (i in 0 until msg.timestamps.size)
            ready.metadata[i].timestamp = msg.timestamps[i]

        logger.info("PARENT-METADATA ${ready.metadata.joinToString(":", prefix = "[", postfix = "]")}")
    }

    override fun parentConnectionLost(host: Host, cause: Throwable?) {
        assertOrExit(state is ParentSync || state is ParentReady, "Connection lost while not connected  $state")
        parentMatchesOrExit(host)

        val some = state as Node
        logger.warn("Connection lost to parent $host: $cause, reconnecting")
        state = ParentConnecting(some.parent, some.grandparents)
        openConnection(some.parent)
    }

    override fun parentConnectionFailed(host: Host, cause: Throwable?) {
        assertOrExit(state is ParentConnecting, "Connection failed while not connecting $state")
        parentMatchesOrExit(host)
        logger.warn("Connection failed to parent $host: $cause")
        val connecting = state as ParentConnecting
        if (connecting.retries < MAX_RECONNECT_RETRIES) {
            state = ParentConnecting(connecting.parent, connecting.grandparents, connecting.retries + 1)
            openConnection(host)
            logger.info("Reconnecting to parent $host, retry ${(state as ParentConnecting).retries}")
        } else {
            tryNextParentOrQuit()
        }
    }

    override fun onReject(host: Host) {
        assertOrExit(state is Node, "Reject while no parent $state")
        parentMatchesOrExit(host)

        val connected = state as ConnectedNode
        closeConnection(connected.parent)

        tryNextParentOrQuit()
    }

    private fun tryNextParentOrQuit() {
        val nodeState = state as Node
        if (nodeState.grandparents.isNotEmpty()) {
            val newParent = nodeState.grandparents[0]
            logger.info("Trying to connect to backup parent $newParent")
            newParent(newParent, nodeState.grandparents.drop(1))
        } else {
            logger.info("No more backups, will deactivate myself!")
            // kill myself
            // This should only happen if I am still trying to connect to a node (aka have no backups)
            // Else, the root should always be available and connectable
            state = Inactive()
            val reject = Reject()
            for (childState in children.values)
                sendMessage(reject, childState.child, TCPChannel.CONNECTION_IN)
            // Send notification to Manager
            triggerNotification(DeactivateNotification())
        }
    }

    /* ------------------------------- CHILD HANDLERS ------------------------------------------- */
    override fun onChildConnected(child: Host) {
        children[child] = ChildState(child)
        logger.info("CHILD SYNC $child")
        if (state is Inactive) {
            sendMessage(Reject(), child, TCPChannel.CONNECTION_IN)
            logger.info("Rejecting child $child")
        }
    }

    override fun onSyncRequest(child: Host, msg: SyncRequest) {
        val childState = children[child]!!
        if (childState.state != ChildState.State.SYNC)
            throw AssertionError("Sync message while already synced $child")

        updateTsAndStableTs()
        sendMessage(SyncResponse(buildReconfigurationMessage(), ByteArray(0)), child, TCPChannel.CONNECTION_IN)
        childState.state = ChildState.State.READY
        logger.info("CHILD READY $child")
        onUpstream(child, msg.upstream)
    }

    override fun onUpstream(child: Host, msg: Upstream) {
        children[child]!!.childStableTime = msg.ts
        logger.info("CHILD-METADATA $child ${msg.ts}")
    }

    override fun onChildDisconnected(child: Host) {
        children.remove(child)!!
        logger.info("CHILD DISCONNECTED $child")
    }

    /* ------------------------------- TIMERS ------------------------------------------- */
    override fun propagateTime() {
        updateTsAndStableTs()
        if (state is ParentReady)
            sendMessage(Upstream(stableTimestamp), (state as ParentReady).parent, TCPChannel.CONNECTION_OUT)

        if (state is ParentReady || state is Datacenter) {
            val downstream = buildDownstreamMessage()
            for (childState in children.values) {
                if (childState.state == ChildState.State.READY)
                    sendMessage(downstream, childState.child, TCPChannel.CONNECTION_IN)
            }
        }
    }

    /* ------------------------------------ UTILS ------------------------------------------------ */

    private fun buildDownstreamMessage(): Downstream {
        val timestamps = mutableListOf<HybridTimestamp>()
        timestamps.add(stableTimestamp)
        if (state is ParentReady) {
            for (p in (state as ParentReady).metadata)
                timestamps.add(p.timestamp)
        }
        return Downstream(timestamps)
    }

    private fun buildReconfigurationMessage(): Reconfiguration {
        val grandparents = mutableListOf<Host>()
        if (state is ParentReady) {
            grandparents.add((state as ParentReady).parent)
            for (p in (state as ParentReady).grandparents)
                grandparents.add(p)
        }
        return Reconfiguration(grandparents, buildDownstreamMessage())
    }


    private fun updateTsAndStableTs() {
        timestamp = timestamp.updatedTs()
        var newStable = timestamp
        for (child in children.values)
            newStable = child.childStableTime.min(newStable)
        stableTimestamp = newStable
    }

    private fun parentMatchesOrExit(parent: Host) {
        when (state) {
            is ParentConnecting -> assertOrExit(parent == (state as ParentConnecting).parent, "Parent mismatch")
            is ParentSync -> assertOrExit(parent == (state as ParentSync).parent, "Parent mismatch")
            is ParentReady -> assertOrExit(parent == (state as ParentReady).parent, "Parent mismatch")
            is Datacenter -> assertOrExit(false, "Parent mismatch")
            is Inactive -> assertOrExit(false, "Parent mismatch")
            else -> assertOrExit(false, "Unexpected parent state $state")
        }
    }

    private fun assertOrExit(condition: Boolean, msg: String) {
        if (!condition) {
            logger.error(msg, AssertionError())
            exitProcess(1)
        }
    }

    override fun onMessageFailed(msg: ProtoMessage, to: Host, cause: Throwable) {
        logger.warn("Message $msg to $to failed: ${cause.localizedMessage}")
    }

}