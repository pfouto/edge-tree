package manager

import Config
import getTimeMillis
import hyparflood.HyParFlood
import ipc.*
import manager.utils.BroadcastState
import manager.utils.BroadcastTimer
import manager.utils.ChildTimer
import manager.messaging.WakeMessage
import org.apache.logging.log4j.LogManager
import pt.unl.fct.di.novasys.babel.core.GenericProtocol
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage
import pt.unl.fct.di.novasys.channel.tcp.TCPChannel
import pt.unl.fct.di.novasys.channel.tcp.events.*
import pt.unl.fct.di.novasys.network.data.Host
import java.net.Inet4Address
import java.util.*

class Manager(private val selfAddress: Inet4Address, config: Config) : GenericProtocol(NAME, ID) {

    companion object {
        const val NAME = "Manager"
        const val ID: Short = 100

        private const val PORT = 20100

        private val logger = LogManager.getLogger()
    }

    enum class State {
        INACTIVE, ACTIVE
    }

    private var state: State = State.INACTIVE
        private set(value) {
            field = value
            logger.info("MANAGER-STATE $value")
        }

    private val channel: Int

    private val region: String
    private val regionalDatacenter: String
    private val amDatacenter: Boolean

    private val location: Pair<Int, Int> = Pair(0, 0)
    private val resources: Int = 0

    private val membership: MutableMap<Inet4Address, Pair<BroadcastState, Long>>

    private val broadcastInterval: Long
    private val membershipExpiration: Long

    private var childTimer = -1L

    init {
        val channelProps = Properties()
        channelProps.setProperty(TCPChannel.ADDRESS_KEY, selfAddress.hostAddress)
        channelProps.setProperty(TCPChannel.PORT_KEY, PORT.toString())
        channelProps.setProperty(TCPChannel.TRIGGER_SENT_KEY, "true")
        channel = createChannel(TCPChannel.NAME, channelProps)

        registerMessageSerializer(channel, WakeMessage.ID, WakeMessage.serializer)
        registerMessageHandler(channel, WakeMessage.ID, this::onWakeMessage, this::onWakeSent, this::onMessageFailed)

        registerChannelEventHandler(channel, OutConnectionFailed.EVENT_ID, this::onOutConnectionFailed)
        registerChannelEventHandler(channel, OutConnectionDown.EVENT_ID, this::onOutConnectionDown)
        registerChannelEventHandler(channel, InConnectionDown.EVENT_ID, this::onInConnectionDown)
        registerChannelEventHandler(channel, OutConnectionUp.EVENT_ID, this::onOutConnectionUp)
        registerChannelEventHandler(channel, InConnectionUp.EVENT_ID, this::onInConnectionUp)

        registerReplyHandler(BroadcastReply.ID, this::onBroadcastReply)
        subscribeNotification(DeactivateNotification.ID) { _: DeactivateNotification, _ -> onDeactivate() }
        subscribeNotification(ActivateNotification.ID) { _: ActivateNotification, _ -> onActivate() }


        registerTimerHandler(BroadcastTimer.TIMER_ID, this::onBroadcastTimer)
        registerTimerHandler(ChildTimer.ID, this::onChildTimer)

        region = config.region
        regionalDatacenter = config.datacenter
        amDatacenter = regionalDatacenter == config.hostname

        broadcastInterval = config.man_broadcast_interval
        membershipExpiration = broadcastInterval * 3

        membership = mutableMapOf()

        logger.info("Region $region, amDc $amDatacenter, regionalDc: $regionalDatacenter")
    }

    override fun init(props: Properties) {
        if (amDatacenter) {
            logger.warn("Starting as datacenter")
            triggerNotification(ActivateNotification(null))
            sendRequest(InitRequest(null), HyParFlood.ID)
        } else {
            logger.warn("Starting asleep")
            sendRequest(InitRequest(Inet4Address.getByName(regionalDatacenter) as Inet4Address), HyParFlood.ID)
        }
        logger.info("Bind address $selfAddress")

        setupPeriodicTimer(BroadcastTimer(), 0, broadcastInterval)
    }

    private fun onActivate() {
        state = State.ACTIVE
        sendRequest(BroadcastRequest(BroadcastState(selfAddress, location, resources, state)), HyParFlood.ID)

        childTimer = setupPeriodicTimer(ChildTimer(), 5000, 10000)
    }

    private fun onDeactivate(){
        state = State.INACTIVE
        sendRequest(BroadcastRequest(BroadcastState(selfAddress, location, resources, state)), HyParFlood.ID)
        if(childTimer!=-1L){
            cancelTimer(childTimer)
            childTimer = -1
        }
    }

    private fun onWakeMessage(msg: WakeMessage, from: Host, sourceProto: Short, channelId: Int) {
        logger.info("$msg FROM $from. Sending to tree")
        if (state == State.INACTIVE)
            triggerNotification(ActivateNotification(msg.contact))
        else
            logger.warn("Received wake message while not dormant, ignoring")
    }

    private fun onChildTimer(timer: ChildTimer, timerId: Long) {
        if (state == State.INACTIVE)
            return

        val sortedFilter = membership.filterValues { it.first.state == State.INACTIVE }
            .toSortedMap(compareBy { it.hostAddress })
        if (!sortedFilter.isEmpty()) {
            val toWake = Host(sortedFilter.keys.first(), PORT)
            logger.info("Waking up $toWake")
            openConnection(toWake)
            sendMessage(WakeMessage(selfAddress), toWake)
        }
    }

    private fun onBroadcastTimer(timer: BroadcastTimer, timerId: Long) {
        sendRequest(BroadcastRequest(BroadcastState(selfAddress, location, resources, state)), HyParFlood.ID)
        val time = getTimeMillis()
        membership.filterValues { it.second < time }.forEach {
            membership.remove(it.key)
            logger.info("MEMBERSHIP remove ${it.key}")
        }
    }

    private fun onBroadcastReply(reply: BroadcastReply, from: Short) {
        val newState = BroadcastState.fromByteArray(reply.payload)
        val newExpiry = getTimeMillis() + membershipExpiration

        val existingPair = membership[newState.address]

        membership[newState.address] = Pair(newState, newExpiry)
        if (existingPair == null || newState != existingPair.first) {
            logger.info("MEMBERSHIP update $newState $newExpiry")
        }
    }

    private fun onWakeSent(msg: WakeMessage, host: Host, destProto: Short, channelId: Int) {
        logger.info("$msg TO $host, closing connection")
        closeConnection(host)
    }

    private fun onMessageFailed(msg: ProtoMessage, to: Host, destProto: Short, cause: Throwable, channelId: Int) {
        logger.warn("Message $msg to $to failed: ${cause.localizedMessage}")
    }

    private fun onOutConnectionUp(event: OutConnectionUp, channelId: Int) {
        logger.debug("Connected out to ${event.node}")
    }

    private fun onOutConnectionFailed(event: OutConnectionFailed<ProtoMessage>, channelId: Int) {
        logger.warn("Failed connecting out to ${event.node}: ${event.cause.localizedMessage}")
    }

    private fun onOutConnectionDown(event: OutConnectionDown, channelId: Int) {
        logger.warn("Lost connection out to ${event.node}")
    }

    private fun onInConnectionUp(event: InConnectionUp, channelId: Int) {
        logger.debug("Connection in up from ${event.node}")
    }

    private fun onInConnectionDown(event: InConnectionDown, channelId: Int) {
        logger.debug("Connection in down from ${event.node}")
    }
}