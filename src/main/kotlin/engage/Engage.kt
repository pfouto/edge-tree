package engage

import Config
import engage.messaging.*
import engage.timers.*
import ipc.*
import org.apache.logging.log4j.LogManager
import pt.unl.fct.di.novasys.babel.core.GenericProtocol
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage
import pt.unl.fct.di.novasys.channel.tcp.TCPChannel
import pt.unl.fct.di.novasys.channel.tcp.events.*
import pt.unl.fct.di.novasys.network.data.Host
import java.net.Inet4Address
import java.util.*
import kotlin.streams.toList

const val DEFAULT_PEER_PORT = 1600

//TODO receive VC from storage and propagate downstream
//TODO receive VC from upstream and send to storage


class Engage(val address: Inet4Address, props: Properties, private val config: Config) :
    GenericProtocol("Engage", 100) {

    private val logger = LogManager.getLogger()

    private val self: Host

    private val peerChannel: Int

    private val reconnectInterval: Long = props.getProperty("reconnect_interval", "5000").toLong()
    private val gossipInterval: Long = props.getProperty("gossip_interval", "5000").toLong()

    private val mfEnabled: Boolean
    private val mfTimeoutMs: Long


    private val partitions: List<String> = config.engPartitions.split(",")
    private val neighbours: MutableMap<Host, NeighState>


    private var nUpdateNotMessages: Int
    private var nMetadataFlushMessages: Int

    private var active: Boolean
    private var amDc: Boolean

    init {

        subscribeNotification(ActivateNotification.ID) { not: ActivateNotification, _ -> onActivate(not) }

        active = false
        amDc = false

        mfEnabled = props.getProperty("mf_enabled", "true").toBoolean()
        mfTimeoutMs = props.getProperty("mf_timeout_ms", "1000").toLong()

        nUpdateNotMessages = 0
        nMetadataFlushMessages = 0

        self = Host(address, DEFAULT_PEER_PORT)
        val peerProps = Properties()
        peerProps.setProperty(TCPChannel.ADDRESS_KEY, address.hostAddress)
        peerProps.setProperty(TCPChannel.PORT_KEY, DEFAULT_PEER_PORT.toString())
        peerProps.setProperty(TCPChannel.TRIGGER_SENT_KEY, "false")
        peerProps.setProperty(TCPChannel.CONNECT_TIMEOUT_KEY, "5000")
        peerProps.setProperty(TCPChannel.HEARTBEAT_INTERVAL_KEY, "5000")
        peerProps.setProperty(TCPChannel.HEARTBEAT_TOLERANCE_KEY, "50000")
        peerChannel = createChannel(TCPChannel.NAME, peerProps)

        neighbours = mutableMapOf()
    }

    override fun init(props: Properties?) {
        registerChannelEventHandler(peerChannel, OutConnectionFailed.EVENT_ID, this::onOutConnectionFailed)
        registerChannelEventHandler(peerChannel, OutConnectionDown.EVENT_ID, this::onOutConnectionDown)
        registerChannelEventHandler(peerChannel, InConnectionDown.EVENT_ID, this::onInConnectionDown)
        registerChannelEventHandler(peerChannel, OutConnectionUp.EVENT_ID, this::onOutConnectionUp)
        registerChannelEventHandler(peerChannel, InConnectionUp.EVENT_ID, this::onInConnectionUp)

        registerMessageSerializer(peerChannel, GossipMessage.MSG_ID, GossipMessage.serializer)
        registerMessageSerializer(peerChannel, MetadataFlush.MSG_ID, MetadataFlush.serializer)
        registerMessageSerializer(peerChannel, UpdateNot.MSG_ID, UpdateNot.serializer)

        registerMessageHandler(peerChannel, GossipMessage.MSG_ID, this::onGossipMessage, this::onMessageFailed)
        registerMessageHandler(peerChannel, UpdateNot.MSG_ID, this::onPeerUpdateNot, this::onMessageFailed)
        registerMessageHandler(peerChannel, MetadataFlush.MSG_ID, this::onPeerMetadataFlush, this::onMessageFailed)

        registerRequestHandler(UpdateNotRequest.ID) { req: UpdateNotRequest, _ -> onClientUpdateNot(req) }

        registerTimerHandler(ReconnectTimer.TIMER_ID, this::onReconnectTimer)
        registerTimerHandler(GossipTimer.TIMER_ID, this::onGossipTimer)
        registerTimerHandler(FlushTimer.TIMER_ID, this::onFlushTimer)

        setupPeriodicTimer(GossipTimer(), 1500 + gossipInterval, gossipInterval)

        logger.info(
            "Engage started, me $self, mf ${if (mfEnabled) "YES" else "NO"}" +
                    "${if (mfEnabled) ", mfTo $mfTimeoutMs" else ""}, neighs: " +
                    "${neighbours.keys.stream().map { s -> s.address.hostAddress.substring(10) }.toList()}"
        )
    }

    private fun onActivate(notification: ActivateNotification) {
        logger.info("$notification received")
        if (active) {
            logger.warn("Already active, ignoring")
            return
        }
        active = true

        if (notification.contact != null) {
            val contact = notification.contact!!
            val host = Host(contact, DEFAULT_PEER_PORT)
            logger.info("Starting and adding neighbor contact $host")
            neighbours[host] = NeighState()
            setupTimer(ReconnectTimer(host), 500)
        } else {
            amDc = true
            logger.info("Am datacenter")
        }
    }

    private fun onGossipTimer(timer: GossipTimer, uId: Long) {
        if (logger.isDebugEnabled)
            logger.debug("Gossip: $neighbours")
        neighbours.filter { it.value.connected }.keys.forEach { sendMessage(peerChannel, buildMessageFor(it), it) }
    }

    private fun buildMessageFor(t: Host): GossipMessage {
        val map: MutableMap<String, Pair<Host, Int>> = mutableMapOf()
        partitions.forEach { map[it] = Pair(self, 1) }
        neighbours.filter { it.key != t }.forEach { (_, pI) ->
            pI.partitions.forEach { (p, pair) ->
                if (map[p] == null || map[p]!!.second > (pair.second + 1))
                    map[p] = Pair(pair.first, pair.second + 1)
            }
        }
        return GossipMessage(map)
    }

    private fun onGossipMessage(msg: GossipMessage, from: Host, sourceProto: Short, channelId: Int) {
        if (logger.isDebugEnabled)
            logger.debug("Received $msg from ${from.address.hostAddress}")
        val info = neighbours[from] ?: throw AssertionError("Not in neighbours list: $from")
        info.partitions = msg.parts
    }

    private fun onFlushTimer(timer: FlushTimer, uId: Long) {
        if (!mfEnabled) throw AssertionError("Received $timer but mf are disabled")
        if (logger.isDebugEnabled)
            logger.debug("Flush: ${timer.edge}")
        val neighState = neighbours[timer.edge]!!
        if (neighState.pendingMF != null) {
            nMetadataFlushMessages++
            sendMessage(peerChannel, neighState.pendingMF, timer.edge)
            neighState.pendingMF = null
        }
    }

    private fun propagateUN(msg: UpdateNot, sourceEdge: Host?) {
        neighbours.forEach { (neigh, nState) ->
            if (neigh != sourceEdge) {
                if (msg.part == "migration" || nState.partitions.containsKey(msg.part)) {
                    //Direct propagate (and flush MF if enabled)
                    val toSend: UpdateNot
                    if (mfEnabled && nState.pendingMF != null) {
                        toSend = msg.copyMergingMF(nState.pendingMF!!)
                        nState.pendingMF = null
                        cancelTimer(nState.timerId)
                    } else toSend = msg
                    nUpdateNotMessages++
                    sendMessage(peerChannel, toSend, neigh)
                } else if (mfEnabled) {
                    if (mfTimeoutMs > 0) {
                        //Either merge msg to mf, or store and create timer
                        if (nState.pendingMF != null) {
                            if (msg.mf != null)
                                nState.pendingMF!!.merge(msg.mf)
                            nState.pendingMF!!.merge(msg.source, msg.vUp)
                        } else {
                            nState.pendingMF = MetadataFlush.single(msg.source, msg.vUp)
                            if (msg.mf != null)
                                nState.pendingMF!!.merge(msg.mf)
                            nState.timerId = setupTimer(FlushTimer(neigh), mfTimeoutMs)
                        }
                    } else {
                        val mf = MetadataFlush.single(msg.source, msg.vUp)
                        if (msg.mf != null) mf.merge(msg.mf)
                        nMetadataFlushMessages++
                        sendMessage(peerChannel, mf, neigh)
                    }
                }
            }
        }
    }

    private fun onClientUpdateNot(req: UpdateNotRequest) {
        if (logger.isDebugEnabled)
            logger.debug("Received ${req.update} from client")
        propagateUN(req.update, null)
    }

    private fun onPeerUpdateNot(msg: UpdateNot, from: Host, sourceProto: Short, channelId: Int) {
        if (logger.isDebugEnabled)
            logger.debug("Received $msg from peer ${from.address.hostAddress}")
        if (!neighbours.containsKey(from)) throw AssertionError("Msg from unknown neigh $from")
        propagateUN(msg, from)

        if (msg.part == "migration" || partitions.contains(msg.part)) {
            sendReply(UpdateNotReply(msg), EngageStorage.ID)
        } else if (mfEnabled) {
            val single = MetadataFlush.single(msg.source, msg.vUp)
            if (msg.mf != null) single.merge(msg.mf)
            sendReply(MFReply(single), EngageStorage.ID)
        }
    }

    private fun onPeerMetadataFlush(msg: MetadataFlush, from: Host, sourceProto: Short, channelId: Int) {
        if (!mfEnabled) throw AssertionError("Received $msg but mf are disabled")
        if (logger.isDebugEnabled)
            logger.debug("Received $msg from ${from.address.hostAddress}")
        if (!neighbours.containsKey(from)) throw AssertionError("Msg from unknown neigh $from")

        neighbours.forEach { (neigh, nState) ->
            if (neigh != from) {
                val toSend: MetadataFlush
                if (nState.pendingMF != null) {
                    toSend = nState.pendingMF!!
                    toSend.merge(msg)
                    nState.pendingMF = null
                    cancelTimer(nState.timerId)
                } else {
                    toSend = msg
                }
                nMetadataFlushMessages++
                sendMessage(peerChannel, toSend, neigh)
            }
        }
        sendReply(MFReply(msg), EngageStorage.ID)
    }

    private fun onMessageFailed(msg: ProtoMessage, to: Host, destProto: Short, cause: Throwable, channelId: Int) {
        logger.warn("Message $msg to $to failed: ${cause.localizedMessage}")
    }

    private fun onOutConnectionUp(event: OutConnectionUp, channelId: Int) {
        if (logger.isDebugEnabled)
            logger.debug("Connected out to ${event.node}")
        val info = neighbours[event.node] ?: throw AssertionError("Not in neighbours list: ${event.node}")
        info.connected = true
    }

    private fun onOutConnectionFailed(event: OutConnectionFailed<ProtoMessage>, channelId: Int) {
        logger.warn("Failed connecting out to ${event.node}: ${event.cause.localizedMessage}, retrying in $reconnectInterval")
        setupTimer(ReconnectTimer(event.node), reconnectInterval)
    }

    private fun onReconnectTimer(timer: ReconnectTimer, uId: Long) {
        if (logger.isDebugEnabled)
            logger.debug("Connecting out to ${timer.node}")
        openConnection(timer.node, peerChannel)
    }

    private fun onOutConnectionDown(event: OutConnectionDown, channelId: Int) {
        logger.warn("Lost connection out to ${event.node}, reconnecting in $reconnectInterval")
        setupTimer(ReconnectTimer(event.node), reconnectInterval)
        val info = neighbours[event.node] ?: throw AssertionError("Not in neighbours list: ${event.node}")
        info.connected = false
    }

    private fun onInConnectionUp(event: InConnectionUp, channelId: Int) {
        if (logger.isDebugEnabled)
            logger.debug("Connection in up from ${event.node}")

        if (!neighbours.containsKey(event.node)) {
            logger.info("Adding neighbor that connected to me ${event.node}")
            neighbours[event.node] = NeighState()
            setupTimer(ReconnectTimer(event.node), 500)
        }
    }

    private fun onInConnectionDown(event: InConnectionDown, channelId: Int) {
        if (logger.isDebugEnabled)
            logger.warn("Connection in down from ${event.node}")
    }

    fun finalLogs() {
        logger.info(
            "Number of message: {} {} {}",
            nUpdateNotMessages + nMetadataFlushMessages, nUpdateNotMessages, nMetadataFlushMessages
        )
    }

    data class NeighState(
        var connected: Boolean = false, var partitions: Map<String, Pair<Host, Int>> = mutableMapOf(),
        var pendingMF: MetadataFlush? = null, var timerId: Long = -1,
    ) {
        override fun toString(): String {
            return "(${if (connected) "up" else "down"}, ${partitions.keys}" +
                    "${if (pendingMF != null) ", mf=$pendingMF" else ""}${if (timerId != -1L) ", tid=$timerId" else ""})"
        }
    }
}