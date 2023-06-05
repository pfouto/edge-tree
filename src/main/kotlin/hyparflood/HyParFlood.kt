package hyparflood

import Config
import hyparflood.messaging.*
import hyparflood.utils.*
import ipc.*
import manager.Manager
import org.apache.logging.log4j.LogManager
import pt.unl.fct.di.novasys.babel.core.GenericProtocol
import pt.unl.fct.di.novasys.channel.tcp.TCPChannel
import pt.unl.fct.di.novasys.channel.tcp.events.*
import pt.unl.fct.di.novasys.network.data.Host
import java.net.Inet4Address
import java.util.*
import kotlin.math.min


class HyParFlood(address: Inet4Address, config: Config) : GenericProtocol(NAME, ID) {
    companion object {
        const val NAME = "HyParFlood"
        const val ID: Short = 300

        private const val PORT = 2300

        private const val MAX_BACKOFF: Int = 60000
        private val logger = LogManager.getLogger()
    }

    private val myself: Host

    private val ARWL: Short //param: active random walk length
    private val PRWL: Short //param: passive random walk length
    private val shuffleTime: Short //param: timeout for shuffle
    private val originalTimeout: Short //param: timeout for hello messages
    private var timeout: Short //param: timeout for hello msgs
    private val joinTimeout: Short //param: timeout for retry join
    private val kActive: Short //param: number of active nodes to exchange on shuffle
    private val kPassive: Short //param: number of passive nodes to exchange on shuffle

    private var active: IView
    private var passive: IView

    private var pending: MutableSet<Host>
    private val activeShuffles: MutableMap<Short, Array<Host>>

    private var seqNum: Short = 0

    private val rnd: Random

    private val highestBroadcastPerHost: MutableMap<Host, Int> = mutableMapOf()
    private var mySeqNumber: Int = 0

    init {
        myself = Host(address, PORT)

        ARWL = config.hpf_arwl
        PRWL = config.hpf_prwl

        shuffleTime = config.hpf_shuffle_time
        timeout = config.hpf_hello_backoff
        originalTimeout = timeout

        joinTimeout = config.hpf_join_timeout
        kActive = config.hpf_k_active
        kPassive = config.hpf_k_passive

        rnd = Random()
        val maxActive: Int = config.hpf_active_view
        val maxPassive: Int = config.hpf_passive_view
        active = View("Active", maxActive, myself, rnd)
        passive = View("Passive", maxPassive, myself, rnd)

        pending = HashSet()
        activeShuffles = TreeMap()

        active.setOther(passive, pending)
        passive.setOther(active, pending)

        val channelProps = Properties()
        channelProps.setProperty(TCPChannel.ADDRESS_KEY, myself.address.hostAddress)
        channelProps.setProperty(TCPChannel.PORT_KEY, myself.port.toString())
        channelProps.setProperty(TCPChannel.TRIGGER_SENT_KEY, "true")
        val channelId = createChannel(TCPChannel.NAME, channelProps)

        /*---------------------- Register Message Serializers ---------------------- */
        registerMessageSerializer(channelId, JoinMessage.MSG_CODE, JoinMessage.serializer)
        registerMessageSerializer(channelId, JoinReplyMessage.MSG_CODE, JoinReplyMessage.serializer)
        registerMessageSerializer(channelId, ForwardJoinMessage.MSG_CODE, ForwardJoinMessage.serializer)
        registerMessageSerializer(channelId, HelloMessage.MSG_CODE, HelloMessage.serializer)
        registerMessageSerializer(channelId, HelloReplyMessage.MSG_CODE, HelloReplyMessage.serializer)
        registerMessageSerializer(channelId, DisconnectMessage.MSG_CODE, DisconnectMessage.serializer)
        registerMessageSerializer(channelId, ShuffleMessage.MSG_CODE, ShuffleMessage.serializer)
        registerMessageSerializer(channelId, ShuffleReplyMessage.MSG_CODE, ShuffleReplyMessage.serializer)
        registerMessageSerializer(channelId, BroadcastMessage.MSG_CODE, BroadcastMessage.serializer)

        /*---------------------- Register Message Handlers -------------------------- */
        registerMessageHandler(channelId, JoinMessage.MSG_CODE, this::uponReceiveJoin)
        registerMessageHandler(channelId, JoinReplyMessage.MSG_CODE, this::uponReceiveJoinReply)
        registerMessageHandler(channelId, ForwardJoinMessage.MSG_CODE, this::uponReceiveForwardJoin)
        registerMessageHandler(channelId, HelloMessage.MSG_CODE, this::uponReceiveHello)
        registerMessageHandler(channelId, HelloReplyMessage.MSG_CODE, this::uponReceiveHelloReply)
        registerMessageHandler(
            channelId,
            DisconnectMessage.MSG_CODE,
            this::uponReceiveDisconnect,
            this::uponDisconnectSent
        )
        registerMessageHandler(channelId, ShuffleMessage.MSG_CODE, this::uponReceiveShuffle)
        registerMessageHandler(
            channelId,
            ShuffleReplyMessage.MSG_CODE,
            this::uponReceiveShuffleReply,
            this::uponShuffleReplySent
        )

        registerMessageHandler(channelId, BroadcastMessage.MSG_CODE, this::uponReceiveBroadcast)


        /*--------------------- Register Timer Handlers ----------------------------- */
        registerTimerHandler(ShuffleTimeout.TIMER_ID, this::uponShuffleTimeout)
        registerTimerHandler(HelloTimeout.TIMER_ID, this::uponHelloTimeout)
        registerTimerHandler(JoinTimeout.TIMER_ID, this::uponJoinTimeout)

        /*-------------------- Register Channel Event ------------------------------- */
        registerChannelEventHandler(channelId, OutConnectionDown.EVENT_ID, this::uponOutConnectionDown)
        registerChannelEventHandler(channelId, OutConnectionFailed.EVENT_ID, this::uponOutConnectionFailed)
        registerChannelEventHandler(channelId, OutConnectionUp.EVENT_ID, this::uponOutConnectionUp)
        registerChannelEventHandler(channelId, InConnectionUp.EVENT_ID, this::uponInConnectionUp)
        registerChannelEventHandler(channelId, InConnectionDown.EVENT_ID, this::uponInConnectionDown)

        registerRequestHandler(InitRequest.ID) { req: InitRequest, _ -> onInitRequest(req) }
        registerRequestHandler(BroadcastRequest.ID) { req: BroadcastRequest, _ -> onBroadcastRequest(req) }

        logger.debug("Bind address {}", myself)

    }

    private fun onInitRequest(request: InitRequest) {
        if (request.address != null) {
            val contactHost = Host(request.address, PORT)
            logger.info("Initializing, contact: $contactHost")
            openConnection(contactHost)
            val m = JoinMessage()
            sendMessage(m, contactHost)
            logger.debug("Sent JoinMessage to {}", contactHost)
            logger.trace("Sent {} to {}", m, contactHost)

            setupTimer(JoinTimeout(contactHost), joinTimeout.toLong())
        } else
            logger.info("Initializing, no contact")

        setupPeriodicTimer(ShuffleTimeout(), shuffleTime.toLong(), shuffleTime.toLong())
    }

    override fun init(props: Properties) {
    }

    private fun onBroadcastRequest(request: BroadcastRequest) {
        val msg = BroadcastMessage(myself, mySeqNumber++, request.payload)
        active.getPeers().forEach { sendMessage(msg, it) }
    }

    private fun uponReceiveBroadcast(msg: BroadcastMessage, from: Host, sourceProto: Short, channelId: Int) {
        logger.debug("Received {} from {}", msg, from)
        if(msg.origin == myself) return

        val highestSeq = highestBroadcastPerHost[msg.origin]
        if (highestSeq == null || msg.seqNumber > highestSeq) {
            highestBroadcastPerHost[msg.origin] = msg.seqNumber
            sendReply(BroadcastReply(msg.payload), Manager.ID)
            active.getPeers().filter { it != from }.forEach { sendMessage(msg, it) }
        }
    }


    private fun handleDropFromActive(dropped: Host?) {
        if (dropped != null) {
            triggerNotification(NeighbourDown(dropped))
            sendMessage(DisconnectMessage(), dropped)
            logger.debug("Sent DisconnectMessage to {}", dropped)
            passive.addPeer(dropped)
            logger.trace("Added to {} passive{}", dropped, passive)
        }
    }

    private fun uponReceiveJoin(msg: JoinMessage, from: Host, sourceProto: Short, channelId: Int) {
        logger.debug("Received {} from {}", msg, from)
        addHostToActiveAndReplyToJoin(from)
        for (peer in active.getPeers()) {
            if (peer != from) {
                sendMessage(ForwardJoinMessage(ARWL, from), peer)
                logger.debug("Sent ForwardJoinMessage to {}", peer)
            }
        }
    }

    private fun uponReceiveJoinReply(msg: JoinReplyMessage, from: Host, sourceProto: Short, channelId: Int) {
        logger.debug("Received {} from {}", msg, from)
        if (!active.containsPeer(from)) {
            passive.removePeer(from)
            pending.remove(from)
            openConnection(from)
            val h = active.addPeer(from)
            logger.trace("Added to {} active{}", from, active)
            triggerNotification(NeighbourUp(from))
            handleDropFromActive(h)
        }
    }

    private fun uponReceiveForwardJoin(msg: ForwardJoinMessage, from: Host, sourceProto: Short, channelId: Int) {
        logger.debug("Received {} from {}", msg, from)
        val newHost = msg.newHost
        if (msg.ttl.toInt() == 0 || active.getPeers().size == 1) {
            if (newHost != myself && !active.containsPeer(newHost)) {
                passive.removePeer(newHost)
                pending.remove(newHost)
                addHostToActiveAndReplyToJoin(newHost)
            }
        } else {
            if (msg.decrementTtl() == PRWL) {
                passive.addPeer(newHost)
                logger.trace("Added to {} passive {}", newHost, passive)
            }
            val next = active.getRandomDiff(from)
            if (next != null) {
                sendMessage(msg, next)
                logger.debug("Sent ForwardJoinMessage to {}", next)
            }
        }
    }

    private fun uponReceiveHello(msg: HelloMessage, from: Host, sourceProto: Short, channelId: Int) {
        logger.debug("Received {} from {}", msg, from)
        openConnection(from)
        if (msg.isPriority) {
            sendMessage(HelloReplyMessage(true), from)
            logger.debug("Sent HelloReplyMessage to {}", from)
            if (!active.containsPeer(from)) {
                pending.remove(from)
                logger.trace("Removed from {} pending{}", from, pending)
                passive.removePeer(from)
                logger.trace("Removed from {} passive{}", from, passive)
                val h = active.addPeer(from)
                logger.trace("Added to {} active{}", from, active)
                triggerNotification(NeighbourUp(from))
                handleDropFromActive(h)
            }
        } else {
            pending.remove(from)
            logger.trace("Removed from {} pending{}", from, pending)
            if (!active.fullWithPending(pending) || active.containsPeer(from)) {
                sendMessage(HelloReplyMessage(true), from)
                logger.debug("Sent HelloReplyMessage to {}", from)
                if (!active.containsPeer(from)) {
                    passive.removePeer(from)
                    logger.trace("Removed from {} passive{}", from, passive)
                    active.addPeer(from)
                    logger.trace("Added to {} active{}", from, active)
                    triggerNotification(NeighbourUp(from))
                }
            } else {
                sendMessage(HelloReplyMessage(false), from, TCPChannel.CONNECTION_IN)
                logger.debug("Sent HelloReplyMessage to {}", from)
            }
        }
    }

    private fun uponReceiveHelloReply(msg: HelloReplyMessage, from: Host, sourceProto: Short, channelId: Int) {
        logger.debug("Received {} from {}", msg, from)
        pending.remove(from)
        logger.trace("Removed from {} pending{}", from, pending)
        if (msg.isTrue) {
            if (!active.containsPeer(from)) {
                timeout = originalTimeout
                openConnection(from)
                val h = active.addPeer(from)
                logger.trace("Added to {} active{}", from, active)
                triggerNotification(NeighbourUp(from))
                handleDropFromActive(h)
            }
        } else if (!active.containsPeer(from)) {
            passive.addPeer(from)
            closeConnection(from)
            logger.trace("Added to {} passive{}", from, passive)
            if (!active.fullWithPending(pending)) {
                setupTimer(HelloTimeout(), timeout.toLong())
            }
        }
    }

    private fun uponReceiveDisconnect(msg: DisconnectMessage, from: Host, sourceProto: Short, channelId: Int) {
        logger.debug("Received {} from {}", msg, from)
        if (active.containsPeer(from)) {
            active.removePeer(from)
            logger.debug("Removed from {} active{}", from, active)
            handleDropFromActive(from)
            if (active.getPeers().isEmpty()) {
                timeout = originalTimeout
            }
            if (!active.fullWithPending(pending)) {
                setupTimer(HelloTimeout(), timeout.toLong())
            }
        }
    }

    private fun uponDisconnectSent(msg: DisconnectMessage, host: Host, destProto: Short, channelId: Int) {
        logger.trace("Sent {} to {}", msg, host)
        closeConnection(host)
    }

    private fun uponReceiveShuffle(msg: ShuffleMessage, from: Host, sourceProto: Short, channelId: Int) {
        logger.debug("Received {} from {}", msg, from)
        openConnection(from)
        msg.decrementTtl()
        if (msg.ttl > 0 && active.getPeers().size > 1) {
            val next = active.getRandomDiff(from)
            sendMessage(msg, next)
            logger.debug("Sent ShuffleMessage to {}", next)
        } else if (msg.getOrigin() != myself) {
            logger.trace("Processing {}, passive{}", msg, passive)
            val peers: MutableSet<Host> = HashSet()
            peers.addAll(active.getRandomSample(msg.getFullSample().size))
            val hosts = peers.toTypedArray()
            var i = 0
            for (host in msg.getFullSample()) {
                if (host != myself && !active.containsPeer(host) && passive.isFull() && i < peers.size) {
                    passive.removePeer(hosts[i])
                    i++
                }
                passive.addPeer(host)
            }
            logger.trace("After Passive{}", passive)
            sendMessage(ShuffleReplyMessage(peers, msg.seqnum), msg.getOrigin())
            logger.debug("Sent ShuffleReplyMessage to {}", msg.getOrigin())
        } else activeShuffles.remove(msg.seqnum)
    }

    private fun uponShuffleReplySent(msg: ShuffleReplyMessage, host: Host, destProto: Short, channelId: Int) {
        if (!active.containsPeer(host) && !pending.contains(host)) {
            logger.trace("Disconnecting from {} after shuffleReply", host)
            closeConnection(host)
        }
    }

    private fun uponReceiveShuffleReply(msg: ShuffleReplyMessage, from: Host, sourceProto: Short, channelId: Int) {
        logger.debug("Received {} from {}", msg, from)
        val sent = activeShuffles.remove(msg.seqnum)
        val sample = mutableListOf<Host>()
        sample += msg.getSample()
        sample += from
        var i = 0
        logger.trace("Processing {}, passive{}", msg, passive)
        for (h in sample) {
            if (h != myself && !active.containsPeer(h) && passive.isFull() && i < sent!!.size) {
                passive.removePeer(sent[i])
                i++
            }
            passive.addPeer(h)
        }
        logger.trace("After Passive{}", passive)
    }

    /*--------------------------------- Timers ---------------------------------------- */

    private fun uponShuffleTimeout(timer: ShuffleTimeout, timerId: Long) {
        if (!active.fullWithPending(pending)) {
            setupTimer(HelloTimeout(), timeout.toLong())
        }
        val h = active.getRandom()
        if (h != null) {
            val peers: MutableSet<Host> = HashSet()
            peers.addAll(active.getRandomSample(kActive.toInt()))
            peers.addAll(passive.getRandomSample(kPassive.toInt()))
            activeShuffles[seqNum] = peers.toTypedArray()
            sendMessage(ShuffleMessage(myself, peers, PRWL, seqNum), h)
            logger.debug("Sent ShuffleMessage to {}", h)
            seqNum = ((seqNum % Short.MAX_VALUE).toShort() + 1).toShort()
        }
    }

    private fun uponHelloTimeout(timer: HelloTimeout, timerId: Long) {
        if (!active.fullWithPending(pending)) {
            val h = passive.dropRandom()
            if (h != null && pending.add(h)) {
                openConnection(h)
                logger.trace(
                    "Sending HelloMessage to {}, pending {}, active {}, passive {}",
                    h, pending, active, passive
                )
                sendMessage(HelloMessage(getPriority()), h)
                logger.debug("Sent HelloMessage to {}", h)
                timeout = min(timeout * 2, MAX_BACKOFF).toShort()
            } else if (h != null) passive.addPeer(h)
        }
    }

    private fun uponJoinTimeout(timer: JoinTimeout, timerId: Long) {
        if (active.isEmpty()) {
            val contact = timer.contact
            openConnection(contact)
            logger.warn("Retrying join to {}", contact)
            val m = JoinMessage()
            sendMessage(m, contact)
            logger.debug("Sent JoinMessage to {}", contact)
            timer.incCount()
            setupTimer(timer, (joinTimeout * timer.count).toLong())
        }
    }

    /*--------------------------------- Procedures ---------------------------------------- */

    private fun getPriority(): Boolean {
        return active.getPeers().size + pending.size == 1
    }

    private fun addHostToActiveAndReplyToJoin(from: Host) {
        openConnection(from)
        val h = active.addPeer(from)
        logger.trace("Added to {} active{}", from, active)
        sendMessage(JoinReplyMessage(), from)
        logger.debug("Sent JoinReplyMessage to {}", from)
        triggerNotification(NeighbourUp(from))
        handleDropFromActive(h)
    }

    /* --------------------------------- Channel Events ---------------------------- */

    private fun uponOutConnectionDown(event: OutConnectionDown, channelId: Int) {
        logger.info("Host {} is down, active{}, cause: {}", event.node, active, event.cause)
        if (active.removePeer(event.node)) {
            triggerNotification(NeighbourDown(event.node))
            if (!active.fullWithPending(pending)) {
                setupTimer(HelloTimeout(), timeout.toLong())
            }
        } else pending.remove(event.node)
    }

    private fun uponOutConnectionFailed(event: OutConnectionFailed<*>, channelId: Int) {
        logger.info("Connection to host {} failed, cause: {}", event.node, event.cause)
        if (active.removePeer(event.node)) {
            triggerNotification(NeighbourDown(event.node))
            if (!active.fullWithPending(pending)) {
                setupTimer(HelloTimeout(), timeout.toLong())
            }
        } else pending.remove(event.node)
    }

    private fun uponOutConnectionUp(event: OutConnectionUp, channelId: Int) {
        logger.trace("Host (out) {} is up", event.node)
    }

    private fun uponInConnectionUp(event: InConnectionUp, channelId: Int) {
        logger.trace("Host (in) {} is up", event.node)
    }

    private fun uponInConnectionDown(event: InConnectionDown, channelId: Int) {
        logger.trace("Connection from host {} is down, active{}, cause: {}", event.node, active, event.cause)
    }
}