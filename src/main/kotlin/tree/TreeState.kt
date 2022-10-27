package tree

import getTimeMillis
import org.apache.logging.log4j.LogManager
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage
import pt.unl.fct.di.novasys.network.data.Host
import tree.utils.HybridTimestamp
import tree.utils.messaging.DownstreamMetadata
import tree.utils.messaging.SyncRequest
import tree.utils.messaging.SyncResponse
import tree.utils.messaging.UpstreamMetadata
import java.util.*
import java.util.function.Consumer

class TreeState {

    enum class ParentState {
        NONE, CONNECTING, SYNC, READY
    }

    companion object {
        private val logger = LogManager.getLogger()
    }

    private var timestamp: HybridTimestamp = HybridTimestamp(getTimeMillis(), 0)
    private var stableTimestamp: HybridTimestamp = HybridTimestamp(0, 0)

    private var parentState: ParentState = ParentState.NONE
    lateinit var parent: Pair<Host, Consumer<ProtoMessage>>
    private lateinit var parents: List<Pair<Host, HybridTimestamp>>

    private val children: MutableMap<Host, ChildState> = mutableMapOf()

    /* ------------------------------- PARENT HANDLERS ------------------------------------------- */
    fun newParent(parent: Host, channelProxy: Consumer<ProtoMessage>) {
        this.parent = Pair(parent, channelProxy)
        parentState = ParentState.CONNECTING
        logger.info("PARENT CONNECTING $parent")
    }

    fun parentConnected(host: Host) {
        checkParentMatches(host)
        if (parentState != ParentState.CONNECTING)
            throw AssertionError("ConnectionUp while not connecting $parent $parentState")
        parentState = ParentState.SYNC
        parent.second.accept(SyncRequest(updateAndGetStableTimestamp(), mutableListOf()))
        logger.info("PARENT SYNC ${parent.first}")
    }

    fun parentSyncResponse(host: Host, msg: SyncResponse) {
        checkParentMatches(host)
        if (parentState != ParentState.SYNC)
            throw AssertionError("Sync resp while not sync $parent $parentState")

        val newParents: MutableList<Pair<Host, HybridTimestamp>> = mutableListOf()
        newParents.add(Pair(host, msg.stableTS))
        newParents.addAll(msg.parents)
        parents = newParents

        parentState = ParentState.READY
        logger.info("PARENT READY ${parent.first}")
    }

    fun downstreamMetadata(host: Host, msg: DownstreamMetadata) {
        try {
            checkParentMatches(host)
            val newParents: MutableList<Pair<Host, HybridTimestamp>> = mutableListOf()
            newParents.add(Pair(host, msg.stableTS))
            newParents.addAll(msg.parents)
            parents = newParents
        } catch (e : Throwable) {
            logger.error("Error while processing downstream metadata from $host", e)
        }
    }

    fun parentConnectionLost(host: Host) {
        if (parentState == ParentState.NONE || parentState == ParentState.CONNECTING)
            throw AssertionError("ConnectionLost while not connected $host $parent $parentState")
        checkParentMatches(host)
        parentState = ParentState.NONE
        logger.info("PARENT DISCONNECTED ${parent.first}")
    }

    /* ------------------------------- CHILD HANDLERS ------------------------------------------- */
    fun childConnected(child: Host, channelProxy: Consumer<ProtoMessage>) {
        children[child] = ChildState(child, channelProxy)
        logger.info("CHILD SYNC $child")
    }

    fun onSyncRequest(child: Host, msg: SyncRequest) {
        val childState = children[child]!!
        if (childState.state != ChildState.State.SYNC)
            throw AssertionError("Sync message while already synced $child")
        childState.proxy.accept(SyncResponse(updateAndGetStableTimestamp(), copyParents(), ByteArray(0)))
        childState.state = ChildState.State.READY
        logger.info("CHILD READY $child")
    }

    fun upstreamMetadata(child: Host, msg: UpstreamMetadata) {
        children[child]!!.childStableTime = msg.ts
    }

    fun childDisconnected(child: Host) {
        children.remove(child)!!
        logger.info("CHILD DISCONNECTED $child")
    }

    /* ------------------------------- TIMERS ------------------------------------------- */
    fun propagateTime() {
        val stableTS = updateAndGetStableTimestamp()
        if (amConnected())
            parent.second.accept(UpstreamMetadata(stableTS))

        for (child in children.values) {
            if (child.state == ChildState.State.READY) {
                child.proxy.accept(DownstreamMetadata(stableTS, copyParents()))
            }
        }

        val sb = StringBuilder()
        for (child in children.values) {
            sb.append("${child.child}-${child.childStableTime};")
        }
        val pSb = StringBuilder()
        if (amConnected()) {
            for (p in parents) {
                pSb.append("${p.first}-${p.second};")
            }
        }
        logger.info("METADATA $timestamp $stableTS C_$sb P_$pSb")
    }

    /* ------------------------------------ UTILS ------------------------------------------------ */
    private fun amConnected(): Boolean {
        return parentState == ParentState.READY
    }

    private fun checkParentMatches(parent: Host) {
        if (parentState == ParentState.NONE)
            throw AssertionError("Parent state mismatch $parent NONE")
        if (parent != this.parent.first)
            throw AssertionError("Parent mismatch $parent $this.parent")
        if (amConnected() && parents[0].first != parent)
            throw AssertionError("Parent mismatch in list $parent ${parents[0]}")
    }

    private fun updateAndGetStableTimestamp(): HybridTimestamp {
        timestamp = timestamp.updatedTs()
        var newStable = timestamp
        for (child in children.values)
            newStable = child.childStableTime.min(newStable)
        stableTimestamp = newStable
        return stableTimestamp
    }

    private fun copyParents(): List<Pair<Host, HybridTimestamp>> {
        if (!amConnected())
            return Collections.emptyList()
        return parents.map { Pair(it.first, it.second) }
    }
}