package tree

import ipc.ActivateNotification
import org.apache.logging.log4j.LogManager
import pt.unl.fct.di.novasys.babel.core.GenericProtocol
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage
import pt.unl.fct.di.novasys.channel.tcp.TCPChannel
import pt.unl.fct.di.novasys.channel.tcp.events.*
import pt.unl.fct.di.novasys.network.data.Host
import tree.utils.PropagateTimer
import tree.utils.ReconnectTimer
import tree.messaging.down.Downstream
import tree.messaging.down.Reconfiguration
import tree.messaging.down.Reject
import tree.messaging.down.SyncResponse
import tree.messaging.up.SyncRequest
import tree.messaging.up.Upstream
import java.net.Inet4Address
import java.util.*

abstract class TreeProto(address: Inet4Address, props: Properties) : GenericProtocol(NAME, ID) {

    companion object {
        const val NAME = "Tree"
        const val ID: Short = 200
        const val PORT = 2200
        const val RECONNECT_TIMEOUT_KEY = "reconnect_timeout"
        const val RECONNECT_TIMEOUT_DEFAULT = "3000"
        const val PROPAGATE_TIMEOUT_KEY = "propagate_timeout"
        const val PROPAGATE_TIMEOUT_DEFAULT = "2000"

        private val logger = LogManager.getLogger()
    }

    private val reconnectTimeout: Long
    private val propagateTimeout: Long
    private val self: Host

    private val channel: Int


    init {
        reconnectTimeout = props.getProperty(RECONNECT_TIMEOUT_KEY, RECONNECT_TIMEOUT_DEFAULT).toLong()
        propagateTimeout = props.getProperty(PROPAGATE_TIMEOUT_KEY, PROPAGATE_TIMEOUT_DEFAULT).toLong()

        self = Host(address, PORT)

        val channelProps = Properties()
        channelProps.setProperty(TCPChannel.ADDRESS_KEY, self.address.hostAddress)
        channelProps.setProperty(TCPChannel.PORT_KEY, self.port.toString())
        channelProps.setProperty(TCPChannel.TRIGGER_SENT_KEY, "true")
        channel = createChannel(TCPChannel.NAME, channelProps)

        registerChannelEventHandler(channel, OutConnectionUp.EVENT_ID)
        { event: OutConnectionUp, _ -> parentConnected(event.node) }

        registerChannelEventHandler(channel, OutConnectionFailed.EVENT_ID)
        { event: OutConnectionFailed<ProtoMessage>, _ -> parentConnectionFailed(event.node, event.cause) }

        registerChannelEventHandler(channel, OutConnectionDown.EVENT_ID)
        { event: OutConnectionDown, _ -> parentConnectionLost(event.node, event.cause) }

        registerChannelEventHandler(channel, InConnectionUp.EVENT_ID)
        { event: InConnectionUp, _: Int -> onChildConnected(event.node) }

        registerChannelEventHandler(channel, InConnectionDown.EVENT_ID)
        { event: InConnectionDown, _: Int -> onChildDisconnected(event.node) }

        registerMessageSerializer(channel, SyncRequest.ID, SyncRequest.Serializer)
        registerMessageSerializer(channel, SyncResponse.ID, SyncResponse.Serializer)
        registerMessageSerializer(channel, Upstream.ID, Upstream.Serializer)
        registerMessageSerializer(channel, Downstream.ID, Downstream.Serializer)
        registerMessageSerializer(channel, Reject.ID, Reject.Serializer)
        registerMessageSerializer(channel, Reconfiguration.ID, Reconfiguration.Serializer)

        registerMessageHandler(
            channel,
            SyncRequest.ID,
            { msg: SyncRequest, from: Host, _, _ -> onSyncRequest(from, msg) },
            { msg: SyncRequest, to: Host, _, cause: Throwable, _ -> onMessageFailed(msg, to, cause) }
        )

        registerMessageHandler(
            channel, SyncResponse.ID,
            { msg: SyncResponse, from, _, _ -> onParentSyncResponse(from, msg) },
            { msg: SyncResponse, to: Host, _, cause: Throwable, _ -> onMessageFailed(msg, to, cause) }
        )
        registerMessageHandler(
            channel, Upstream.ID,
            { msg: Upstream, from, _, _ -> onUpstream(from, msg) },
            { msg: Upstream, to: Host, _, cause: Throwable, _ -> onMessageFailed(msg, to, cause) }
        )

        registerMessageHandler(
            channel, Downstream.ID,
            { msg: Downstream, from, _, _ -> onDownstream(from, msg) },
            { msg: Downstream, to: Host, _, cause: Throwable, _ -> onMessageFailed(msg, to, cause) }
        )

        registerMessageHandler(
            channel, Reject.ID,
            { _, from, _, _ -> onReject(from) },
            { msg: Reject, to: Host, _, cause: Throwable, _ -> onMessageFailed(msg, to, cause) }
        )

        registerMessageHandler(
            channel, Reconfiguration.ID,
            { msg: Reconfiguration, from, _, _ -> onReconfiguration(from, msg) },
            { msg: Reconfiguration, to: Host, _, cause: Throwable, _ -> onMessageFailed(msg, to, cause) }
        )

        subscribeNotification(ActivateNotification.ID) { not: ActivateNotification, _ -> activate(not) }

        registerTimerHandler(ReconnectTimer.ID) { timer: ReconnectTimer, _ -> openConnection(timer.node) }
        registerTimerHandler(PropagateTimer.ID) { _: PropagateTimer, _ -> propagateTime() }
    }

    override fun init(props: Properties) {
        logger.info("Bind address $self")

        setupPeriodicTimer(PropagateTimer(), propagateTimeout, propagateTimeout)
    }

    abstract fun activate(notification: ActivateNotification)
    abstract fun parentConnected(host: Host)
    abstract fun onParentSyncResponse(host: Host, msg: SyncResponse)
    abstract fun onReconfiguration(host: Host, reconfiguration: Reconfiguration)
    abstract fun onDownstream(host: Host, msg: Downstream)
    abstract fun parentConnectionLost(host: Host, cause: Throwable?)
    abstract fun parentConnectionFailed(host: Host, cause: Throwable?)
    abstract fun onReject(host: Host)
    abstract fun onChildConnected(child: Host)
    abstract fun onSyncRequest(child: Host, msg: SyncRequest)
    abstract fun onUpstream(child: Host, msg: Upstream)
    abstract fun onChildDisconnected(child: Host)
    abstract fun propagateTime()
    abstract fun onMessageFailed(msg: ProtoMessage, to: Host, cause: Throwable)
}