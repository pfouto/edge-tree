package tree

import Config
import getTimeMillis
import ipc.*
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
    private fun newParent(
        parent: Host,
        backups: List<Host> = mutableListOf(),
        dataRequests: MutableSet<Pair<String, String>> = mutableSetOf(),
    ) {
        state = ParentConnecting(parent, backups, dataRequests)
        openConnection(parent)
    }

    override fun parentConnected(host: Host) {
        val oldState = state as ParentConnecting
        assertOrExit(host == oldState.parent, "Parent mismatch")
        state = ParentSync.fromConnecting(oldState)

        updateTsAndStableTs()
        //TODO get list of all data from storage (probably have to track it here too)
        sendMessage(SyncRequest(Upstream(stableTimestamp), mutableSetOf()), host, TCPChannel.CONNECTION_OUT)
    }

    override fun onParentSyncResponse(host: Host, msg: SyncResponse) {
        val oldState = state as ParentSync
        assertOrExit(host == oldState.parent, "Parent mismatch")

        state = ParentReady(host, emptyList(), emptyList(), oldState.dataRequests)
        onReconfiguration(host, msg.reconfiguration)

        //TODO re-request data to new (or not) parent

    }

    override fun onReconfiguration(host: Host, reconfiguration: Reconfiguration) {
        val oldState = state as ParentReady
        assertOrExit(host == oldState.parent, "Parent mismatch")

        val metadata = mutableListOf<Metadata>()
        for (i in 0 until reconfiguration.grandparents.size + 1)
            metadata.add(Metadata(HybridTimestamp()))

        state = ParentReady(host, reconfiguration.grandparents, metadata, oldState.dataRequests)

        onDownstream(host, reconfiguration.downstream)

        val reconfigurationMessage = buildReconfigurationMessage()
        for (childState in children.values)
            sendMessage(reconfigurationMessage, childState.child, TCPChannel.CONNECTION_IN)
    }

    override fun onDownstream(host: Host, msg: Downstream) {
        val ready = state as ParentReady
        assertOrExit(host == ready.parent, "Parent mismatch")

        assertOrExit(msg.timestamps.size == ready.grandparents.size + 1, "Wrong number of timestamps")
        assertOrExit(msg.timestamps.size == ready.metadata.size, "Wrong number of timestamps")

        for (i in 0 until msg.timestamps.size)
            ready.metadata[i].timestamp = msg.timestamps[i]

        logger.info("PARENT-METADATA ${ready.metadata.joinToString(":", prefix = "[", postfix = "]")}")
    }

    override fun parentConnectionLost(host: Host, cause: Throwable?) {
        val some = state as Node
        assertOrExit(some is ParentSync || some is ParentReady, "Connection lost while not connected  $some")
        assertOrExit(host == some.parent, "Parent mismatch")

        logger.warn("Connection lost to parent $host: $cause, reconnecting")
        state = ParentConnecting(some.parent, some.grandparents, some.dataRequests)
        openConnection(some.parent)
    }

    override fun parentConnectionFailed(host: Host, cause: Throwable?) {
        val old = state as ParentConnecting
        assertOrExit(host == old.parent, "Parent mismatch")

        logger.warn("Connection failed to parent $host: $cause")
        if (old.retries < MAX_RECONNECT_RETRIES) {
            state = ParentConnecting(old.parent, old.grandparents, old.dataRequests, old.retries + 1)
            openConnection(host)
            logger.info("Reconnecting to parent $host, retry ${(state as ParentConnecting).retries}")
        } else {
            tryNextParentOrQuit()
        }
    }

    override fun onReject(host: Host) {
        val connected = state as ConnectedNode
        assertOrExit(host == connected.parent, "Parent mismatch")

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
        children[child] = ChildSync(child)
        logger.info("CHILD SYNC $child")
        if (state is Inactive) {
            sendMessage(Reject(), child, TCPChannel.CONNECTION_IN)
            logger.info("Rejecting child $child")
        }
    }

    override fun onSyncRequest(child: Host, msg: SyncRequest) {
        val childState = children[child]!!
        if (childState !is ChildSync)
            throw AssertionError("Sync message while already synced $child")

        updateTsAndStableTs()
        sendMessage(SyncResponse(buildReconfigurationMessage(), ByteArray(0)), child, TCPChannel.CONNECTION_IN)
        children[child] = ChildReady(child)
        logger.info("CHILD READY $child")
        onUpstream(child, msg.upstream)
    }

    override fun onUpstream(child: Host, msg: Upstream) {
        (children[child]!! as ChildReady).childStableTime = msg.ts
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
                if (childState is ChildReady)
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
        for (child in children.values.filterIsInstance<ChildReady>())
            newStable = child.childStableTime.min(newStable)
        stableTimestamp = newStable
    }

    override fun onMessageFailed(msg: ProtoMessage, to: Host, cause: Throwable) {
        logger.warn("Message $msg to $to failed: ${cause.localizedMessage}")
    }

    override fun onLocalReplicationRequest(request: LocalReplicationRequest) {
        assertOrExit(state !is Datacenter, "Local replication request while in datacenter mode")
        assertOrExit(state !is Inactive, "Local replication request while inactive")

        val nodeState = state as Node
        nodeState.dataRequests.add(Pair(request.partition, request.key))
        when(state){
            is ParentConnecting, is ParentSync -> {}
            is ParentReady -> {}
        }
    }

    override fun onChildReplicationReply(reply: ChildReplicationReply) {

    }

    override fun onPropagateWriteRequest(request: PropagateWriteRequest) {

    }

    private fun assertOrExit(condition: Boolean, msg: String) {
        if (!condition) {
            logger.error(msg, AssertionError())
            exitProcess(1)
        }
    }


}