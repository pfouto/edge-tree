package tree

import Config
import ipc.*
import org.apache.logging.log4j.LogManager
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage
import pt.unl.fct.di.novasys.channel.tcp.TCPChannel
import pt.unl.fct.di.novasys.network.data.Host
import storage.utils.ChildDataIndex
import storage.RemoteWrite
import storage.Storage
import tree.messaging.down.*
import tree.messaging.up.*
import tree.utils.*
import java.net.Inet4Address
import java.nio.ByteBuffer
import java.util.*
import java.util.function.Supplier
import kotlin.system.exitProcess

class Tree(address: Inet4Address, config: Config, private val timestampReader: Supplier<HybridTimestamp>) :
    TreeProto(address, config) {

    companion object {
        private val logger = LogManager.getLogger()
        const val MAX_RECONNECT_RETRIES = 3
    }

    //Children
    private var stableTimestamp: HybridTimestamp = HybridTimestamp(0, 0)
    private val children: MutableMap<Host, ChildState> = mutableMapOf()

    //Parent
    private var state: State = Inactive()
        private set(value) {
            field = value
            logger.info("TREE-STATE $state")
        }
    private val pendingParentRemoteWrites = mutableListOf<Pair<WriteID, RemoteWrite>>()

    private val ipInt = ByteBuffer.wrap(address.address).getInt()
    private var idCounter = 0

    //Persistence
    private val localPersistenceMapper = TreeMap<Int, Long>()

    //Migrations
    private val parentMigrations = mutableListOf<MigrationRequest>() //Unlocks on downstream

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

    override fun onChildConnected(child: Host) {
        children[child] = ChildConnected(child)
        logger.info("CHILD CONNECTED $child")
        if (state is Inactive) {
            sendMessage(Reject(), child, TCPChannel.CONNECTION_IN)
            logger.info("Rejecting child $child")
        }
    }

    override fun parentConnected(host: Host) {
        val oldState = state as ParentConnecting
        assertOrExit(host == oldState.parent, "Parent mismatch")
        state = ParentConnected(oldState.parent, oldState.grandparents)

        sendRequest(FetchMetadataReq(oldState.parent), Storage.ID)
    }

    override fun onFetchMetadataReply(reply: FetchMetadataRep) {
        logger.debug(
            "Received FetchMetadataRep to parent {}, full: {} partial: {}",
            reply.parent, reply.fullPartitions.keys, reply.partialPartitions.keys
        )
        if (state !is ParentConnected) return
        val oldState = state as ParentConnected
        if (oldState.parent != reply.parent) return

        state = ParentSync(oldState.parent, oldState.grandparents)
        updateStableTs()
        sendMessage(
            SyncRequest(UpstreamMetadata(stableTimestamp), reply.fullPartitions, reply.partialPartitions),
            reply.parent, TCPChannel.CONNECTION_OUT
        )
    }

    override fun onChildSyncRequest(child: Host, msg: SyncRequest) {
        val childState = children[child]!!
        assertOrExit(childState is ChildConnected, "Sync message while not connected $child")

        children[child] = ChildSync(child, ChildDataIndex.fromSyncRequest(msg))
        logger.info("CHILD SYNC $child")

        sendRequest(DataDiffRequest(child, msg), Storage.ID)
        onChildUpstreamMetadata(child, msg.upstream)
    }

    override fun onDataDiffReply(reply: DataDiffReply) {
        val childState = children[reply.child]!! as ChildSync
        children[reply.child] = ChildReady(reply.child, childState.objects, childState.childStableTime)
        logger.info("CHILD READY ${reply.child}")
        sendRequest(AddedChildRequest(reply.child, childState.objects), Storage.ID)
        updateStableTs()
        sendMessage(SyncResponse(buildReconfigurationMessage(), reply.data), reply.child, TCPChannel.CONNECTION_IN)
        sendMessage(DownstreamWrite(childState.pendingWrites), reply.child, TCPChannel.CONNECTION_IN)
    }

    override fun onParentSyncResponse(parent: Host, msg: SyncResponse) {
        val oldState = state as ParentSync
        assertOrExit(parent == oldState.parent, "Parent mismatch")
        state = ParentReady(parent, emptyList(), emptyList())

        //SyncApply also triggers requests for pending data and pending persistence
        sendRequest(SyncApply(msg.items), Storage.ID)

        onReconfiguration(parent, msg.reconfiguration)

        sendMessage(UpstreamWrite(pendingParentRemoteWrites.toList()), parent, TCPChannel.CONNECTION_OUT)
        pendingParentRemoteWrites.clear()

    }

    override fun onReconfiguration(host: Host, reconfiguration: Reconfiguration) {
        val oldState = state as ParentReady
        assertOrExit(host == oldState.parent, "Parent mismatch")

        //Handle reconfiguration locally
        val metadata = reconfiguration.timestamps.map { Metadata(it) }
        state = ParentReady(host, reconfiguration.grandparents, metadata)
        assertOrExit(metadata.size == reconfiguration.grandparents.size + 1, "Wrong number of timestamps")

        //Send new configuration to storage
        val parentList = mutableListOf<Host>()
        parentList.add(host)
        parentList.addAll(reconfiguration.grandparents)
        sendRequest(ReconfigurationApply(parentList), Storage.ID)

        //Send new configuration to children
        val reconfigurationMessage = buildReconfigurationMessage()
        for (childState in children.values)
            if (childState is ChildReady)
                sendMessage(reconfigurationMessage, childState.child, TCPChannel.CONNECTION_IN)
    }

    /* ------------------------------- TIMERS ------------------------------------------- */
    override fun propagateTime() {
        updateStableTs()
        if (state is ParentReady)
            sendMessage(UpstreamMetadata(stableTimestamp), (state as ParentReady).parent, TCPChannel.CONNECTION_OUT)

        if (state is Datacenter) {
            val timestamps = fetchUpstreamTimestamps()
            for (childState in children.values) {
                if (childState is ChildReady) {
                    //Local persistence mappers are empty for datacenter
                    val persistence = mapOf(Int.MAX_VALUE to childState.highestPersistenceIdSeen)
                    sendMessage(DownstreamMetadata(timestamps, persistence), childState.child, TCPChannel.CONNECTION_IN)
                }
            }
        }
    }

    override fun onParentDownstreamMetadata(host: Host, msg: DownstreamMetadata) {
        val ready = state as ParentReady
        assertOrExit(host == ready.parent, "Parent mismatch")
        assertOrExit(msg.timestamps.size == ready.grandparents.size + 1, "Wrong number of timestamps")
        assertOrExit(msg.timestamps.size == ready.metadata.size, "Wrong number of timestamps")

        //Handle timestamps
        for (i in 0 until msg.timestamps.size)
            ready.metadata[i].timestamp = msg.timestamps[i]

        logger.debug("PARENT-METADATA ${ready.metadata.joinToString(":", prefix = "[", postfix = "]")}")

        //Handle local persistence
        val localPersistenceUpdates = mutableMapOf<Int, Long>()
        msg.persistence.forEach { (level, highestOp) ->
            val value = localPersistenceMapper.floorEntry(highestOp)?.value
            if (value != null) {
                localPersistenceUpdates[level] = value
                if (level == Int.MAX_VALUE)
                    localPersistenceMapper.headMap(highestOp, true).clear()
            }
        }
        sendReply(PersistenceUpdate(localPersistenceUpdates), Storage.ID)

        //Handle migrations
        val iterator = parentMigrations.iterator()
        while (iterator.hasNext()) {
            val migration = iterator.next()
            if(getClosestParentTimestamp(migration.migration.path, ready).isAfterOrEqual(migration.migration.hlc)) {
                iterator.remove()
                sendReply(MigrationReply(migration.id), Storage.ID)
            }
        }

        //Handle child persistence
        updateStableTs()
        val timestamps = fetchUpstreamTimestamps()
        for (childState in children.values) {
            if (childState is ChildReady) {
                val childPersistence = mutableMapOf<Int, Int>()
                msg.persistence.forEach { (level, highestOp) ->
                    val value = childState.persistenceMapper.floorEntry(highestOp)?.value
                    if (value != null) {
                        childPersistence[level] = value
                        if (level == Int.MAX_VALUE)
                            childState.persistenceMapper.headMap(highestOp, true).clear()
                    }
                }
                sendMessage(
                    DownstreamMetadata(timestamps, childPersistence),
                    childState.child,
                    TCPChannel.CONNECTION_IN
                )
            }
        }
    }

    private fun fetchUpstreamTimestamps(): List<HybridTimestamp> {
        val timestamps = mutableListOf<HybridTimestamp>()
        timestamps.add(stableTimestamp)
        if (state is ParentReady) {
            for (p in (state as ParentReady).metadata)
                timestamps.add(p.timestamp)
        }
        return timestamps
    }

    private fun buildReconfigurationMessage(): Reconfiguration {
        val grandparents = mutableListOf<Host>()
        if (state is ParentReady) {
            grandparents.add((state as ParentReady).parent)
            for (p in (state as ParentReady).grandparents)
                grandparents.add(p)
        }
        return Reconfiguration(grandparents, fetchUpstreamTimestamps())
    }

    override fun parentConnectionLost(host: Host, cause: Throwable?) {
        val some = state as Node
        assertOrExit(some is ParentSync || some is ParentReady, "Connection lost while not connected  $some")
        assertOrExit(host == some.parent, "Parent mismatch")

        logger.warn("Connection lost to parent $host: $cause, reconnecting")
        state = ParentConnecting(some.parent, some.grandparents)
        openConnection(some.parent)
    }

    override fun parentConnectionFailed(host: Host, cause: Throwable?) {
        val old = state as ParentConnecting
        assertOrExit(host == old.parent, "Parent mismatch")

        logger.warn("Connection failed to parent $host: $cause")
        if (old.retries < MAX_RECONNECT_RETRIES) {
            state = ParentConnecting(old.parent, old.grandparents, old.retries + 1)
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

    override fun onDownstreamWrite(from: Host, msg: DownstreamWrite) {
        msg.writes.forEach {
            logger.debug("Received downstream write for {}", it.second.objectIdentifier)
            sendReply(PropagateWriteReply(it.first, it.second, true), Storage.ID)
        }
        propagateWritesToChildren(msg.writes)
    }

    override fun onUpstreamWrite(child: Host, msg: UpstreamWrite) {
        val childState = children[child]!! as ChildReady

        val transformed = mutableListOf<Pair<WriteID, RemoteWrite>>()

        msg.writes.forEach { (id, write) ->
            val localPersistenceId = idCounter++
            val newId = WriteID(id.ip, id.counter, localPersistenceId)

            logger.debug("Received upstream write for {} from {}", write.objectIdentifier, child)
            sendReply(PropagateWriteReply(newId, write, false), Storage.ID)

            childState.highestPersistenceIdSeen = id.persistence
            if (state !is Datacenter) {
                childState.persistenceMapper[localPersistenceId] = id.persistence
                transformed.add(Pair(newId, write))
            }
        }

        when (val parentState = state) {
            is ParentSync, is ParentConnecting, is ParentConnected -> pendingParentRemoteWrites.addAll(transformed)
            is ParentReady -> sendMessage(UpstreamWrite(transformed), parentState.parent, TCPChannel.CONNECTION_OUT)
        }
    }

    override fun onPropagateLocalWrite(req: PropagateWriteRequest) {
        val opID = idCounter++
        val writeID = WriteID(ipInt, opID, opID)

        if (state !is Datacenter)
            localPersistenceMapper[opID] = req.id

        when (val parentState = state) {
            is ParentSync, is ParentConnecting, is ParentConnected ->
                pendingParentRemoteWrites.add(Pair(writeID, req.write))

            is ParentReady ->
                sendMessage(
                    UpstreamWrite(listOf(Pair(writeID, req.write))),
                    parentState.parent,
                    TCPChannel.CONNECTION_OUT
                )
        }

        propagateWritesToChildren(listOf(Pair(writeID, req.write)))
    }

    private fun propagateWritesToChildren(writes: List<Pair<WriteID, RemoteWrite>>) {
        writes.forEach {
            val writePartition = it.second.objectIdentifier.partition
            children.forEach { (c, state) ->
                when (state) {
                    is ChildSync -> {
                        if (state.objects.containsObject(it.second.objectIdentifier))
                            state.pendingWrites.add(it)
                    }

                    is ChildReady -> {
                        if (state.objects.containsObject(it.second.objectIdentifier))
                            sendMessage(DownstreamWrite(it.first, it.second), c, TCPChannel.CONNECTION_IN)
                        else if (state.pendingFullPartitions.containsKey(writePartition))
                            state.pendingFullPartitions[writePartition]!!.add(it)
                        else if (state.pendingObjects.containsKey(it.second.objectIdentifier))
                            state.pendingObjects[it.second.objectIdentifier]!!.add(it)
                    }
                }
            }
        }
    }

    /* ------------------------------- CHILD HANDLERS ------------------------------------------- */

    override fun onChildUpstreamMetadata(child: Host, msg: UpstreamMetadata) {
        val childState = children[child]!! as ChildMeta
        childState.childStableTime = msg.ts

        //Check pending migrations from this child
        if(childState is ChildReady) {
            val iterator = childState.pendingMigrations.iterator()
            while (iterator.hasNext()) {
                val mig = iterator.next()
                if (msg.ts.isAfterOrEqual(mig.migration.hlc)) {
                    iterator.remove()
                    sendReply(MigrationReply(mig.id), Storage.ID)
                }
            }
        }
        logger.debug("CHILD-METADATA {} {}", child, msg.ts)
    }

    override fun onChildDisconnected(child: Host) {
        val remove = children.remove(child)!!
        if (remove is ChildReady) {
            sendRequest(RemovedChildRequest(child), Storage.ID)
            remove.pendingMigrations.forEach { mig ->
                sendReply(MigrationReply(mig.id), Storage.ID)
            }
        }
        updateStableTs()
        logger.info("CHILD DISCONNECTED $child")
    }

    private fun updateStableTs() {
        var newStable = timestampReader.get()
        for (child in children.values.filterIsInstance<ChildReady>())
            newStable = child.childStableTime.min(newStable)
        stableTimestamp = newStable
    }

    override fun onMessageFailed(msg: ProtoMessage, to: Host, cause: Throwable) {
        //logger.warn("Message $msg to $to failed: ${cause.localizedMessage}")
    }

    override fun onObjectReplicationRequest(request: ObjReplicationReq) {
        val nodeState = state as Node

        if (nodeState is ParentReady) {
            logger.debug("Sending object replication request {} to {}", request.requests, nodeState.parent)
            sendMessage(ObjectReplicationRequest(request.requests), nodeState.parent, TCPChannel.CONNECTION_OUT)
        } else {
            logger.debug("Ignoring object replication request while not ready")
        }
    }

    override fun onPartitionReplicationRequest(req: PartitionReplicationReq) {
        val nodeState = state as Node

        if (nodeState is ParentReady) {
            logger.debug("Sending partition replication request {}", req.partition)
            sendMessage(PartitionReplicationRequest(req.partition), nodeState.parent, TCPChannel.CONNECTION_OUT)
        } else {
            logger.debug("Ignoring partition replication request while not ready")
        }
    }

    override fun onChildObjReplicationRequest(child: Host, msg: ObjectReplicationRequest) {
        val childState = children[child]!! as ChildReady
        msg.items.forEach { childState.pendingObjects.putIfAbsent(it, mutableListOf()) }
        logger.debug("Child {} requested objects {} ", child, msg.items)
        sendRequest(FetchObjectsReq(child, msg.items), Storage.ID)
    }

    override fun onChildPartitionReplicationRequest(from: Host, msg: PartitionReplicationRequest) {
        val childState = children[from]!! as ChildReady
        childState.pendingFullPartitions.putIfAbsent(msg.partition, mutableListOf())
        logger.debug("Child {} requested partition {} ", from, msg.partition)
        sendRequest(FetchPartitionReq(from, msg.partition), Storage.ID)
    }

    override fun onFetchObjectsReply(reply: FetchObjectsRep) {
        val childState = children[reply.child]
        if (childState == null || childState !is ChildReady) {
            logger.warn("Received FetchObjectsReply to invalid child ${reply.child}")
            return
        }

        sendMessage(ObjectReplicationReply(reply.objects), reply.child, TCPChannel.CONNECTION_IN)

        val pendingRemoteWrites = mutableListOf<Pair<WriteID, RemoteWrite>>()
        reply.objects.forEach {
            pendingRemoteWrites.addAll(childState.pendingObjects.remove(it.objectIdentifier)!!)
            childState.objects.addObject(it.objectIdentifier)
        }

        if (pendingRemoteWrites.isNotEmpty())
            sendMessage(DownstreamWrite(pendingRemoteWrites), reply.child, TCPChannel.CONNECTION_IN)
    }

    override fun onFetchPartitionReply(reply: FetchPartitionRep) {
        val childState = children[reply.child]
        if (childState == null || childState !is ChildReady) {
            logger.warn("Received FetchObjectsReply to invalid child ${reply.child}")
            return
        }

        sendMessage(PartitionReplicationReply(reply.partition, reply.objects), reply.child, TCPChannel.CONNECTION_IN)

        childState.objects.addFullPartition(reply.partition)

        val pendingRemoteWrites = childState.pendingFullPartitions.remove(reply.partition)!!
        if (pendingRemoteWrites.isNotEmpty()) {
            sendMessage(DownstreamWrite(pendingRemoteWrites), reply.child, TCPChannel.CONNECTION_IN)
        }
    }

    override fun onParentObjReplicationReply(parent: Host, msg: ObjectReplicationReply) {
        logger.debug("Received object replication reply {} from {}", msg.items.map { it.objectIdentifier }, parent)
        sendReply(ObjReplicationRep(msg.items), Storage.ID)
    }

    override fun onParentPartitionReplicationReply(from: Host, msg: PartitionReplicationReply) {
        logger.debug("Received partition replication reply ${msg.partition}")
        sendReply(PartitionReplicationRep(msg.partition, msg.objects), Storage.ID)
    }

    private fun assertOrExit(condition: Boolean, msg: String) {
        if (!condition) {
            logger.error(msg, AssertionError())
            exitProcess(1)
        }
    }

    override fun onRemoveReplicas(req: RemoveReplicasRequest) {
        if (state is ParentReady)
            sendMessage(
                ReplicaRemovalRequest(req.deletedObjects, req.deletedPartitions),
                (state as ParentReady).parent,
                TCPChannel.CONNECTION_OUT
            )
        else
            logger.warn("Ignoring replica removal request while not ready")
    }

    override fun onReplicaRemoval(child: Host, msg: ReplicaRemovalRequest) {
        val childState = children[child] as ChildReady
        childState.objects.removeAll(msg.deletedObjects, msg.deletedPartitions)
    }

    override fun onMigrationRequest(req: MigrationRequest) {
        if (req.migration.path.contains(self)) {
            //Came from a child node
            for ((child, childState) in children) {
                if (childState is ChildReady && req.migration.path.contains(child)) {
                    //Found the child that we must track
                    if (childState.childStableTime.isAfterOrEqual(req.migration.hlc))
                        sendReply(MigrationReply(req.id), Storage.ID)
                    else
                        childState.pendingMigrations.add(req)
                    return
                }
                // Child not found, probably dead, so we just respond with ok!
                sendReply(MigrationReply(req.id), Storage.ID)
            }
        } else {
            //Came from a different branch
            val myState = state as Node
            if (myState is ParentReady &&
                getClosestParentTimestamp(req.migration.path, myState)
                    .isAfterOrEqual(req.migration.hlc))
                sendReply(MigrationReply(req.id), Storage.ID)
            else
                parentMigrations.add(req)

        }
    }

    private fun getClosestParentTimestamp(clientPath: List<Host>, myState: ParentReady): HybridTimestamp {
        //Must always return something (since at the very least the root is shared)
        if(clientPath.contains(myState.parent))
            return myState.metadata[0].timestamp
        for(i in 1 until myState.grandparents.size) {
            if(clientPath.contains(myState.grandparents[i]))
                return myState.metadata[i+1].timestamp
        }
        throw IllegalStateException("Could not find a common parent")
    }


}