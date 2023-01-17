package tree

import getTimeMillis
import org.apache.logging.log4j.LogManager
import pt.unl.fct.di.novasys.network.data.Host
import tree.utils.HybridTimestamp
import tree.utils.messaging.Downstream
import tree.utils.messaging.SyncRequest
import tree.utils.messaging.SyncResponse
import tree.utils.messaging.Upstream
import java.util.*
import kotlin.system.exitProcess

class TreeState(private val connector: Tree.Connector) {

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
    private var parentState: ParentState = ParentNone()

    /* ------------------------------- PARENT HANDLERS ------------------------------------------- */
    fun newParent(parent: Host, backups: MutableList<Host> = mutableListOf()) {
        assertOrExit(parentState is ParentNone, "Trying to add a new parent when we already have one")
        parentState = ParentConnecting(parent, backups)
        logger.info("$parentState")
        connector.accept(parent)
    }

    fun parentConnected(host: Host, proxy: Tree.MessageSenderOut) {
        assertOrExit(parentState is ParentConnecting, "ConnectionUp while not connecting: $parentState")
        parentMatchesOrExit(host)
        parentState = ParentSync(host, proxy, (parentState as ParentConnecting).backups)
        updateTsAndStableTs()
        proxy.accept(SyncRequest(Upstream(stableTimestamp), mutableListOf()))
        logger.info("$parentState")
    }

    fun parentSyncResponse(host: Host, msg: SyncResponse) {
        assertOrExit(parentState is ParentSync, "Sync resp while not sync $parentState")
        parentMatchesOrExit(host)
        val sync = parentState as ParentSync
        parentState = ParentReady(host, sync.proxy, emptyList(), sync.backups)
        downstream(host, msg.downstream)
        logger.info("$parentState")
    }

    fun downstream(host: Host, msg: Downstream) {
        assertOrExit(parentState is ParentReady, "DownstreamMetadata while not ready $parentState")
        parentMatchesOrExit(host)
        val newParents: MutableList<Pair<Host, HybridTimestamp>> = mutableListOf()
        newParents.add(Pair(host, msg.stableTS))
        newParents.addAll(msg.parents)
        val ready = parentState as ParentReady
        ready.predecessors = newParents
        ready.backups.clear()
        msg.parents.forEach { (parent, _) -> ready.backups.add(parent) }
    }

    fun parentConnectionFailed(host: Host, cause: Throwable?) {
        assertOrExit(parentState is ParentConnecting, "Connection failed while not connecting $parentState")
        parentMatchesOrExit(host)
        logger.warn("Connection failed to parent $host: $cause")
        val connecting = parentState as ParentConnecting
        if (connecting.retries < MAX_RECONNECT_RETRIES) {
            connecting.retries++
            connector.accept(host)
            logger.info("Reconnecting to parent $host, retry ${connecting.retries}")
        } else {
            parentState = ParentNone()
            if (connecting.backups.isNotEmpty()) {
                val newParent = connecting.backups.removeAt(0)
                logger.info("Trying to connect to backup parent $newParent")
                newParent(newParent, backups = connecting.backups)
            } else {
                logger.info("No more backups, going to parentless state")
                parentState = ParentNone()
                //TODO kill myself if I'm not the root
                //TODO should this even happen? ROOT should always be there
            }
        }
    }

    fun parentConnectionLost(host: Host, cause: Throwable?) {
        assertOrExit(parentState is ParentSync || parentState is ParentReady,
            "Connection lost while not connected  $parentState")
        parentMatchesOrExit(host)
        val some = parentState as ParentSome
        logger.warn("Connection lost to parent $host: $cause, reconnecting")
        parentState = ParentConnecting(some.parent, some.backups)
        logger.info("$parentState")
        connector.accept(some.parent)
    }

    /* ------------------------------- CHILD HANDLERS ------------------------------------------- */
    fun childConnected(child: Host, channelProxy: Tree.MessageSenderIn) {
        children[child] = ChildState(child, channelProxy)
        logger.info("CHILD SYNC $child")
    }

    fun onSyncRequest(child: Host, msg: SyncRequest) {
        val childState = children[child]!!
        if (childState.state != ChildState.State.SYNC)
            throw AssertionError("Sync message while already synced $child")
        updateTsAndStableTs()
        childState.proxy.accept(SyncResponse(Downstream(stableTimestamp, copyParents()), ByteArray(0)))
        childState.state = ChildState.State.READY
        upstream(child, msg.upstream)
        logger.info("CHILD READY $child")
    }

    fun upstream(child: Host, msg: Upstream) {
        children[child]!!.childStableTime = msg.ts
    }

    fun childDisconnected(child: Host) {
        children.remove(child)!!
        logger.info("CHILD DISCONNECTED $child")
    }

    /* ------------------------------- TIMERS ------------------------------------------- */
    fun propagateTime() {
        updateTsAndStableTs()
        if (parentState is ParentReady)
            (parentState as ParentReady).proxy.accept(Upstream(stableTimestamp))

        for (child in children.values) {
            if (child.state == ChildState.State.READY) {
                child.proxy.accept(Downstream(stableTimestamp, copyParents()))
            }
        }

        val sb = StringBuilder()
        for (child in children.values) {
            sb.append("${child.child}-${child.childStableTime};")
        }
        val pSb = StringBuilder()
        if (parentState is ParentReady) {
            for (p in (parentState as ParentReady).predecessors) {
                pSb.append("${p.first}-${p.second};")
            }
        }
        logger.info("METADATA $timestamp $stableTimestamp C_$sb P_$pSb")
    }

    /* ------------------------------------ UTILS ------------------------------------------------ */
    private fun updateTsAndStableTs() {
        timestamp = timestamp.updatedTs()
        var newStable = timestamp
        for (child in children.values)
            newStable = child.childStableTime.min(newStable)
        stableTimestamp = newStable
    }

    private fun copyParents(): List<Pair<Host, HybridTimestamp>> {
        if (parentState !is ParentReady)
            return Collections.emptyList()
        return (parentState as ParentReady).predecessors.map { Pair(it.first, it.second) }
    }

    private fun parentMatchesOrExit(parent: Host) {
        when (parentState) {
            is ParentConnecting -> assertOrExit(parent == (parentState as ParentConnecting).parent, "Parent mismatch")
            is ParentSync -> assertOrExit(parent == (parentState as ParentSync).parent, "Parent mismatch")
            is ParentReady -> assertOrExit(parent == (parentState as ParentReady).parent, "Parent mismatch")
            is ParentNone -> assertOrExit(false, "Parent mismatch")
            else -> assertOrExit(false, "Unexpected parent state $parentState")
        }
    }

    private fun assertOrExit(condition: Boolean, msg: String) {
        if (!condition) {
            logger.error(msg, AssertionError())
            exitProcess(1)
        }
    }
}