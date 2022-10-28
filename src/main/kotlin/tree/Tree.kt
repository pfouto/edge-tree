package tree

import manager.ChildRequest
import manager.Manager
import org.apache.logging.log4j.LogManager
import pt.unl.fct.di.novasys.babel.core.GenericProtocol
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage
import pt.unl.fct.di.novasys.channel.tcp.TCPChannel
import pt.unl.fct.di.novasys.channel.tcp.events.*
import pt.unl.fct.di.novasys.network.data.Host
import tree.utils.messaging.*
import java.net.Inet4Address
import java.util.*
import java.util.function.Consumer

class Tree(address: Inet4Address, props: Properties) : GenericProtocol(NAME, ID) {

    companion object {
        const val NAME = "Tree"
        const val ID: Short = 2
        const val PORT = 2901
        const val RECONNECT_TIMEOUT_KEY = "reconrenect_timeout"
        const val RECONNECT_TIMEOUT_DEFAULT = "3000"
        const val PROPAGATE_TIMEOUT_KEY = "propagate_timeout"
        const val PROPAGATE_TIMEOUT_DEFAULT = "2000"

        private val logger = LogManager.getLogger()
    }

    enum class State {
        DORMANT, LEAF, ROOT
    }

    private var state: State = State.DORMANT
    private val treeState: TreeState

    private val reconnectTimeout: Long
    private val propagateTimeout: Long
    private val self: Host

    private val channel: Int

    init {
        reconnectTimeout = props.getProperty(RECONNECT_TIMEOUT_KEY, RECONNECT_TIMEOUT_DEFAULT).toLong()
        propagateTimeout = props.getProperty(PROPAGATE_TIMEOUT_KEY, PROPAGATE_TIMEOUT_DEFAULT).toLong()

        self = Host(address, PORT)
        treeState = TreeState()
        val channelProps = Properties()
        channelProps.setProperty(TCPChannel.ADDRESS_KEY, self.address.hostAddress)
        channelProps.setProperty(TCPChannel.PORT_KEY, self.port.toString())
        channelProps.setProperty(TCPChannel.TRIGGER_SENT_KEY, "true")
        channel = createChannel(TCPChannel.NAME, channelProps)
    }

    override fun init(props: Properties) {
        subscribeNotification(BootstrapNotification.ID, ::onBootstrapNot)

        registerChannelEventHandler(channel, OutConnectionUp.EVENT_ID)
        { event: OutConnectionUp, _ -> treeState.parentConnected(event.node) }

        registerChannelEventHandler(channel, OutConnectionFailed.EVENT_ID, ::onOutConnectionFailed)

        registerChannelEventHandler(channel, OutConnectionDown.EVENT_ID)
        { event: OutConnectionDown, _ -> treeState.parentConnectionLost(event.node) }

        registerChannelEventHandler(channel, InConnectionUp.EVENT_ID)
        { event: InConnectionUp, _: Int -> treeState.childConnected(event.node, MessageSenderIn(event.node)) }
        registerChannelEventHandler(channel, InConnectionDown.EVENT_ID)
        { event: InConnectionDown, _: Int -> treeState.childDisconnected(event.node) }

        registerMessageSerializer(channel, SyncRequest.ID, SyncRequest.Serializer)
        registerMessageSerializer(channel, SyncResponse.ID, SyncResponse.Serializer)
        registerMessageSerializer(channel, UpstreamMetadata.ID, UpstreamMetadata.Serializer)
        registerMessageSerializer(channel, DownstreamMetadata.ID, DownstreamMetadata.Serializer)

        registerMessageHandler(
            channel, SyncRequest.ID,
            { msg: SyncRequest, from, _, _ -> treeState.onSyncRequest(from, msg) },
            ::onMessageFailed
        )
        registerMessageHandler(
            channel, SyncResponse.ID,
            { msg: SyncResponse, from, _, _ -> treeState.parentSyncResponse(from, msg) },
            this::onMessageFailed
        )
        registerMessageHandler(
            channel, UpstreamMetadata.ID,
            { msg: UpstreamMetadata, from, _, _ -> treeState.upstreamMetadata(from, msg) },
            this::onMessageFailed
        )

        registerMessageHandler(
            channel, DownstreamMetadata.ID,
            { msg: DownstreamMetadata, from, _, _ -> treeState.downstreamMetadata(from, msg) },
            this::onMessageFailed
        )

        registerTimerHandler(ReconnectTimer.ID) { timer: ReconnectTimer, _ -> openConnection(timer.node) }
        registerTimerHandler(ChildTimer.ID) { _: ChildTimer, _ -> sendRequest(ChildRequest(), Manager.ID) }
        registerTimerHandler(PropagateTimer.ID) { _: PropagateTimer, _ -> treeState.propagateTime() }

        logger.info("Bind address $self")
    }

    private fun onBootstrapNot(notification: BootstrapNotification, emmiter: Short) {
        logger.info("$notification received")
        val contact = notification.contact
        when (state) {
            State.DORMANT -> {
                if (contact != null) {
                    logger.info("Connecting to $contact")
                    treeState.newParent(contact, MessageSenderOut(contact))
                    openConnection(contact)
                    state = State.LEAF
                    logger.info("STATE LEAF")
                } else {
                    logger.warn("Starting by myself")
                    state = State.ROOT
                    logger.info("STATE ROOT")
                }
                logger.info("Setting up childTimer")
                setupPeriodicTimer(ChildTimer(), 3000, 3000)
                setupPeriodicTimer(PropagateTimer(), propagateTimeout, propagateTimeout)
            }

            State.ROOT -> logger.warn("Am already a root, ignoring")

            State.LEAF -> logger.warn("Am already a leaf, ignoring")
        }
    }

    private fun onMessageFailed(msg: ProtoMessage, to: Host, destProto: Short, cause: Throwable, channelId: Int) {
        logger.warn("Message $msg to $to failed: ${cause.localizedMessage}")
        //TODO redirect to handler?
    }

    private fun onOutConnectionFailed(event: OutConnectionFailed<ProtoMessage>, channelId: Int) {
        logger.warn("Failed connecting out to ${event.node}: ${event.cause.localizedMessage}")

        if (event.node == treeState.parent.first) {
            setupTimer(ReconnectTimer(event.node), reconnectTimeout)
        }
    }

    inner class MessageSenderOut(private val node: Host) : Consumer<ProtoMessage> {
        override fun accept(msg: ProtoMessage) {
            sendMessage(msg, node, TCPChannel.CONNECTION_OUT)
        }
    }

    inner class MessageSenderIn(private val node: Host) : Consumer<ProtoMessage> {
        override fun accept(msg: ProtoMessage) {
            sendMessage(msg, node, TCPChannel.CONNECTION_IN)
        }
    }

}