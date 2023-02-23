package tree

import getTimeMillis
import org.apache.cassandra.config.Config
import org.apache.cassandra.config.YamlConfigurationLoader
import org.apache.cassandra.net.MessagingService
import org.apache.cassandra.service.CassandraDaemon
import org.apache.logging.log4j.LogManager
import pt.unl.fct.di.novasys.network.data.Host
import tree.utils.ActivateNotification
import tree.utils.HybridTimestamp
import tree.utils.messaging.down.Downstream
import tree.utils.messaging.down.Reconfiguration
import tree.utils.messaging.down.Reject
import tree.utils.messaging.down.SyncResponse
import tree.utils.messaging.up.SyncRequest
import tree.utils.messaging.up.Upstream
import java.net.URL
import java.util.function.Supplier
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
    private var state: State = Inactive()
        private set(value){
            field = value
            logger.info("TREE-STATE $state")
        }

    fun activate(notification: ActivateNotification, cassandraConfigMap: Map<String, Any>) {
        logger.info("$notification received")
        if(state !is Inactive){
            logger.warn("Already active, ignoring")
            return
        }


        val config = YamlConfigurationLoader().loadConfig(URL("file:./cassandra.yaml"))
        Config.setOverrideLoadConfig { config }
        //TODO setup override config
        logger.info("Instantiating cassandra")
        System.setProperty("cassandra.storagedir", "/tmp/cassandra")
        CassandraDaemon.getInstanceForTesting().activate()
        MessagingService.instance().inboundSink.add { message ->
            //TODO do something useful here
            println("Received message ${message.verb()} from ${message.from()}")
            true
        }
        //TODO create keyspace and tables
        logger.info("Cassandra instanced")



        if (notification.contact == null) {
            state = Datacenter()
        } else
            newParent(notification.contact)
        connector.sendStateNotification(true)

    }

    /* ------------------------------- PARENT HANDLERS ------------------------------------------- */
    private fun newParent(parent: Host, backups: List<Host> = mutableListOf()) {
        state = ParentConnecting(parent, backups)
        connector.connect(parent)
    }

    fun parentConnected(host: Host, proxy: Tree.ParentProxy) {
        assertOrExit(state is ParentConnecting, "ConnectionUp while not connecting: $state")
        parentMatchesOrExit(host)
        state = ParentSync(host, proxy, (state as ParentConnecting).grandparents)
        updateTsAndStableTs()
        proxy.upMessage(SyncRequest(Upstream(stableTimestamp), mutableListOf()))
    }

    fun onParentSyncResponse(host: Host, msg: SyncResponse) {
        assertOrExit(state is ParentSync, "Sync resp while not sync $state")
        parentMatchesOrExit(host)
        val sync = state as ParentSync
        state = ParentReady(host, sync.proxy, emptyList(), emptyList())
        onReconfiguration(host, msg.reconfiguration)
    }

    fun onReconfiguration(host: Host, reconfiguration: Reconfiguration) {
        assertOrExit(state is ParentReady, "Reconfiguration while not ready $state")
        parentMatchesOrExit(host)

        val ready = state as ParentReady
        val metadata = mutableListOf<Metadata>()
        for(i in 0 until reconfiguration.grandparents.size + 1)
            metadata.add(Metadata(HybridTimestamp()))

        state = ParentReady(host, ready.proxy, reconfiguration.grandparents, metadata)

        onDownstream(host, reconfiguration.downstream)

        val reconfigMsg = buildReconfigurationMessage()
        for(child in children.values)
            child.proxy.downMessage(reconfigMsg)
    }

    fun onDownstream(host: Host, msg: Downstream) {
        assertOrExit(state is ParentReady, "DownstreamMetadata while not ready $state")
        parentMatchesOrExit(host)
        val ready = state as ParentReady
        assertOrExit(msg.timestamps.size == ready.grandparents.size + 1, "Wrong number of timestamps")
        assertOrExit(msg.timestamps.size == ready.metadata.size, "Wrong number of timestamps")

        for(i in 0 until msg.timestamps.size)
            ready.metadata[i].timestamp = msg.timestamps[i]

        logger.info("PARENT-METADATA ${ready.metadata.joinToString(":", prefix = "[", postfix = "]")}")

    }

    fun parentConnectionLost(host: Host, cause: Throwable?) {
        assertOrExit(
            state is ParentSync || state is ParentReady,
            "Connection lost while not connected  $state"
        )
        parentMatchesOrExit(host)
        val some = state as Node
        logger.warn("Connection lost to parent $host: $cause, reconnecting")
        state = ParentConnecting(some.parent, some.grandparents)
        connector.connect(some.parent)
    }

    fun parentConnectionFailed(host: Host, cause: Throwable?) {
        assertOrExit(state is ParentConnecting, "Connection failed while not connecting $state")
        parentMatchesOrExit(host)
        logger.warn("Connection failed to parent $host: $cause")
        val connecting = state as ParentConnecting
        if (connecting.retries < MAX_RECONNECT_RETRIES) {
            state = ParentConnecting(connecting.parent, connecting.grandparents, connecting.retries+1)
            connector.connect(host)
            logger.info("Reconnecting to parent $host, retry ${(state as ParentConnecting).retries}")
        } else {
            tryNextParentOrQuit()
        }
    }

    fun onReject(host: Host) {
        assertOrExit(state is Node, "Reject while no parent $state")
        parentMatchesOrExit(host)
        val connected = state as ConnectedNode
        connected.proxy.closeConnection()
        tryNextParentOrQuit()
    }

    private fun tryNextParentOrQuit(){
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
            for (child in children.values)
                child.proxy.downMessage(Reject())

            // Send notification to Manager
            connector.sendStateNotification(false)
        }
    }

    /* ------------------------------- CHILD HANDLERS ------------------------------------------- */
    fun onChildConnected(child: Host, channelProxy: Tree.ChildProxy) {
        children[child] = ChildState(child, channelProxy)
        logger.info("CHILD SYNC $child")
        if(state is Inactive){
            channelProxy.downMessage(Reject())
            logger.info("Rejecting child $child")
        }
    }

    fun onSyncRequest(child: Host, msg: SyncRequest) {
        val childState = children[child]!!
        if (childState.state != ChildState.State.SYNC)
            throw AssertionError("Sync message while already synced $child")
        updateTsAndStableTs()
        childState.proxy.downMessage(SyncResponse(buildReconfigurationMessage(), ByteArray(0)))
        childState.state = ChildState.State.READY
        onUpstream(child, msg.upstream)
        logger.info("CHILD READY $child")
    }

    fun onUpstream(child: Host, msg: Upstream) {
        children[child]!!.childStableTime = msg.ts
        logger.info("CHILD-METADATA $child ${msg.ts}")
    }

    fun onChildDisconnected(child: Host) {
        children.remove(child)!!
        logger.info("CHILD DISCONNECTED $child")
    }

    /* ------------------------------- TIMERS ------------------------------------------- */
    fun propagateTime() {
        updateTsAndStableTs()
        if (state is ParentReady)
            (state as ParentReady).proxy.upMessage(Upstream(stableTimestamp))

        if(state is ParentReady || state is Datacenter) {
            val downstream = buildDownstreamMessage()
            for (child in children.values) {
                if (child.state == ChildState.State.READY)
                    child.proxy.downMessage(downstream)
            }
        }
    }

    /* ------------------------------------ UTILS ------------------------------------------------ */

    private fun buildDownstreamMessage(): Downstream {
        val timestamps = mutableListOf<HybridTimestamp>()
        timestamps.add(stableTimestamp)
        if(state is ParentReady) {
            for (p in (state as ParentReady).metadata)
                timestamps.add(p.timestamp)
        }
        return Downstream(timestamps)
    }

    private fun buildReconfigurationMessage() : Reconfiguration {
        val grandparents = mutableListOf<Host>()
        if(state is ParentReady) {
            grandparents.add(( state as ParentReady).parent)
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
}