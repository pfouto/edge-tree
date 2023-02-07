package tree

import org.apache.logging.log4j.LogManager
import pt.unl.fct.di.novasys.babel.core.GenericProtocol
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage
import pt.unl.fct.di.novasys.channel.tcp.TCPChannel
import pt.unl.fct.di.novasys.channel.tcp.events.*
import pt.unl.fct.di.novasys.network.data.Host
import tree.utils.ActivateNotification
import tree.utils.StateNotification
import tree.utils.PropagateTimer
import tree.utils.ReconnectTimer
import tree.utils.messaging.down.Downstream
import tree.utils.messaging.down.Reconfiguration
import tree.utils.messaging.down.Reject
import tree.utils.messaging.down.SyncResponse
import tree.utils.messaging.up.SyncRequest
import tree.utils.messaging.up.Upstream
import java.net.Inet4Address
import java.util.*

class Tree(address: Inet4Address, props: Properties) : GenericProtocol(NAME, ID) {

    companion object {
        const val NAME = "Tree"
        const val ID: Short = 201
        const val PORT = 2901
        const val RECONNECT_TIMEOUT_KEY = "reconnect_timeout"
        const val RECONNECT_TIMEOUT_DEFAULT = "3000"
        const val PROPAGATE_TIMEOUT_KEY = "propagate_timeout"
        const val PROPAGATE_TIMEOUT_DEFAULT = "2000"

        private val logger = LogManager.getLogger()
    }

    private val treeState: TreeState

    private val reconnectTimeout: Long
    private val propagateTimeout: Long
    private val self: Host

    private val channel: Int

    init {
        reconnectTimeout = props.getProperty(RECONNECT_TIMEOUT_KEY, RECONNECT_TIMEOUT_DEFAULT).toLong()
        propagateTimeout = props.getProperty(PROPAGATE_TIMEOUT_KEY, PROPAGATE_TIMEOUT_DEFAULT).toLong()

        self = Host(address, PORT)
        treeState = TreeState(Connector())
        val channelProps = Properties()
        channelProps.setProperty(TCPChannel.ADDRESS_KEY, self.address.hostAddress)
        channelProps.setProperty(TCPChannel.PORT_KEY, self.port.toString())
        channelProps.setProperty(TCPChannel.TRIGGER_SENT_KEY, "true")
        channel = createChannel(TCPChannel.NAME, channelProps)
    }

    override fun init(props: Properties) {

        registerChannelEventHandler(channel, OutConnectionUp.EVENT_ID)
        { event: OutConnectionUp, _ -> treeState.parentConnected(event.node, ParentProxy(event.node)) }

        registerChannelEventHandler(channel, OutConnectionFailed.EVENT_ID)
        { event: OutConnectionFailed<ProtoMessage>, _ -> treeState.parentConnectionFailed(event.node, event.cause) }

        registerChannelEventHandler(channel, OutConnectionDown.EVENT_ID)
        { event: OutConnectionDown, _ -> treeState.parentConnectionLost(event.node, event.cause) }

        registerChannelEventHandler(channel, InConnectionUp.EVENT_ID)
        { event: InConnectionUp, _: Int -> treeState.onChildConnected(event.node, ChildProxy(event.node)) }

        registerChannelEventHandler(channel, InConnectionDown.EVENT_ID)
        { event: InConnectionDown, _: Int -> treeState.onChildDisconnected(event.node) }

        registerMessageSerializer(channel, SyncRequest.ID, SyncRequest.Serializer)
        registerMessageSerializer(channel, SyncResponse.ID, SyncResponse.Serializer)
        registerMessageSerializer(channel, Upstream.ID, Upstream.Serializer)
        registerMessageSerializer(channel, Downstream.ID, Downstream.Serializer)
        registerMessageSerializer(channel, Reject.ID, Reject.Serializer)
        registerMessageSerializer(channel, Reconfiguration.ID, Reconfiguration.Serializer)

        registerMessageHandler(
            channel,
            SyncRequest.ID,
            { msg: SyncRequest, from, _, _ -> treeState.onSyncRequest(from, msg) },
            ::onMessageFailed
        )
        registerMessageHandler(
            channel, SyncResponse.ID,
            { msg: SyncResponse, from, _, _ -> treeState.onParentSyncResponse(from, msg) },
            this::onMessageFailed
        )
        registerMessageHandler(
            channel, Upstream.ID,
            { msg: Upstream, from, _, _ -> treeState.onUpstream(from, msg) },
            this::onMessageFailed
        )

        registerMessageHandler(
            channel, Downstream.ID,
            { msg: Downstream, from, _, _ -> treeState.onDownstream(from, msg) },
            this::onMessageFailed
        )

        registerMessageHandler(
            channel, Reject.ID,
            { _, from, _, _ -> treeState.onReject(from) },
            this::onMessageFailed
        )

        registerMessageHandler(
            channel, Reconfiguration.ID,
            { msg: Reconfiguration, from, _, _ -> treeState.onReconfiguration(from, msg) },
            this::onMessageFailed
        )

        subscribeNotification(ActivateNotification.ID) { not: ActivateNotification, _ -> treeState.activate(not) }

        registerTimerHandler(ReconnectTimer.ID) { timer: ReconnectTimer, _ -> openConnection(timer.node) }
        registerTimerHandler(PropagateTimer.ID) { _: PropagateTimer, _ -> treeState.propagateTime() }

        logger.info("Bind address $self")

        setupPeriodicTimer(PropagateTimer(), propagateTimeout, propagateTimeout)
    }

    private fun onMessageFailed(msg: ProtoMessage, to: Host, destProto: Short, cause: Throwable, channelId: Int) {
        logger.warn("Message $msg to $to failed: ${cause.localizedMessage}")
        //TODO redirect TreeState handler?
    }

    inner class ParentProxy(private val node: Host) {
        fun upMessage(msg: ProtoMessage) {
            sendMessage(msg, node, TCPChannel.CONNECTION_OUT)
        }

        fun closeConnection() {
            closeConnection(node, TCPChannel.CONNECTION_OUT)
        }
    }

    inner class ChildProxy(private val node: Host) {
        fun downMessage(msg: ProtoMessage) {
            sendMessage(msg, node, TCPChannel.CONNECTION_IN)
        }
    }

    inner class Connector {
        fun connect(node: Host) {
            openConnection(node)
        }

        fun sendStateNotification(active: Boolean) {
            triggerNotification(StateNotification(active))
        }
    }
}