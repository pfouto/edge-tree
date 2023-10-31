package manager

import Config
import com.charleskorn.kaml.Yaml
import com.charleskorn.kaml.decodeFromStream
import getTimeMillis
import hyparflood.HyParFlood
import ipc.*
import manager.utils.BroadcastState
import manager.utils.BroadcastTimer
import manager.utils.TreeBuilderTimer
import manager.messaging.WakeMessage
import org.apache.logging.log4j.LogManager
import pt.unl.fct.di.novasys.babel.core.GenericProtocol
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage
import pt.unl.fct.di.novasys.channel.tcp.TCPChannel
import pt.unl.fct.di.novasys.channel.tcp.events.*
import pt.unl.fct.di.novasys.network.data.Host
import java.io.FileInputStream
import java.net.Inet4Address
import java.util.*
import kotlin.math.pow
import kotlin.math.sqrt

class Manager(private val selfAddress: Inet4Address, private val config: Config) : GenericProtocol(NAME, ID) {

    companion object {
        const val NAME = "Manager"
        const val ID: Short = 100

        private const val PORT = 20100

        private val logger = LogManager.getLogger()

        val maxDistToCenter = 300
        val wideLevelRanges = arrayOf(1, 75, 180, 100000)
        val wide20LevelRanges = arrayOf(1, 130, 100000)

        val deepLevelRanges = arrayOf(1, 20,45,70,100,130,165,200,235,270, 100000)
    }

    enum class State {
        INACTIVE, ACTIVE
    }

    enum class TreeBuilder {
        Random,  //Parent wakes up a random node
        Static,  //Parent wakes up a child node from a static tree
        Location //Child connects to best parent based on location after parent wakes up
    }


    enum class LocationSub(val orderingFunc: (BroadcastState, Location, Int) -> Double) {
        centralized({ state, _, _-> state.location.distanceToCenter() }),

        deep({ state, myLoc, _ ->
            if (state.location.distanceToCenter() > myLoc.distanceToCenter())
                (0.75 * state.location.distanceToCenter() + state.location.distanceTo(myLoc)) * 10.0
            else
                0.75 * state.location.distanceToCenter() + state.location.distanceTo(myLoc)
        }),
        /*deep({ state, myLoc ->
            val myDistToCenter = myLoc.distanceToCenter()
            val otherDistToCenter = state.location.distanceToCenter()

            val myLevel = deepLevelRanges.indexOfFirst { myDistToCenter < it }
            val otherLevel = deepLevelRanges.indexOfFirst { otherDistToCenter < it }

            val myAngle = myLoc.angle()
            val otherAngle = state.location.angle()

            if (otherDistToCenter > myDistToCenter)
                (0.75 * otherDistToCenter + state.location.distanceTo(myLoc)) * 1000.0
            else if (otherLevel >= myLevel)
                (0.75 * otherDistToCenter + state.location.distanceTo(myLoc)) * 500.0
            else if (Math.abs(myAngle - otherAngle) > 60)
                (0.75 * otherDistToCenter + state.location.distanceTo(myLoc)) * 100.0
            else
                (0.75 * otherDistToCenter + state.location.distanceTo(myLoc)) * (myLevel-otherLevel)
        }),*/
        wide({ state, myLoc, size ->

            val myDistToCenter = myLoc.distanceToCenter()
            val otherDistToCenter = state.location.distanceToCenter()

            val myLevel = if(size > 100) wideLevelRanges.indexOfFirst { myDistToCenter < it } else wide20LevelRanges.indexOfFirst { myDistToCenter < it }
            val otherLevel = if(size > 100) wideLevelRanges.indexOfFirst { otherDistToCenter < it } else wide20LevelRanges.indexOfFirst { otherDistToCenter < it }

            val myAngle = myLoc.angle()
            val otherAngle = state.location.angle()

            if (otherDistToCenter > myDistToCenter)
                (0.75 * otherDistToCenter + state.location.distanceTo(myLoc)) * 1000.0
            else if (otherLevel >= myLevel)
                (0.75 * otherDistToCenter + state.location.distanceTo(myLoc)) * 500.0
            else if (Math.abs(myAngle - otherAngle) > 80)
                (0.75 * otherDistToCenter + state.location.distanceTo(myLoc)) * 100.0
            else
                (0.75 * otherDistToCenter + state.location.distanceTo(myLoc)) * (myLevel-otherLevel)
        })
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

    private val myLocation: Location
    private val resources: Int = 0

    private val membership: MutableMap<Inet4Address, Pair<BroadcastState, Long>>

    private val broadcastInterval: Long
    private val membershipExpiration: Long

    private val treeBuilder: TreeBuilder
    private val staticTree: Map<String, List<String>>?
    private var treeBuilderTimer = -1L
    private val locationSub: LocationSub

    var startTime: Long = Long.MAX_VALUE

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
        registerTimerHandler(TreeBuilderTimer.ID, this::onTreeBuilderTimer)

        region = config.region
        regionalDatacenter = config.datacenter
        amDatacenter = regionalDatacenter == config.hostname

        myLocation = Location(config.locationX, config.locationY)

        broadcastInterval = config.man_broadcast_interval
        membershipExpiration = broadcastInterval * 3

        treeBuilder = TreeBuilder.valueOf(config.tree_builder)
        staticTree = if (treeBuilder == TreeBuilder.Static)
            Yaml.default.decodeFromStream<Map<String, List<String>>>(FileInputStream(config.tree_build_static_location))
        else
            null

        locationSub = LocationSub.valueOf(config.tree_builder_location_sub)

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
        logger.debug("Bind address {}", selfAddress)

        startTime = System.currentTimeMillis()
        setupPeriodicTimer(BroadcastTimer(), 0, broadcastInterval)
        treeBuilderTimer =
            setupPeriodicTimer(TreeBuilderTimer(), config.tree_builder_interval, config.tree_builder_interval)
    }

    private fun onActivate() {
        state = State.ACTIVE
        sendRequest(BroadcastRequest(BroadcastState(selfAddress, myLocation, resources, state)), HyParFlood.ID)
    }

    private fun onDeactivate() {
        state = State.INACTIVE
        sendRequest(BroadcastRequest(BroadcastState(selfAddress, myLocation, resources, state)), HyParFlood.ID)
        if (treeBuilderTimer != -1L) {
            cancelTimer(treeBuilderTimer)
            treeBuilderTimer = -1
        }
    }

    private fun onWakeMessage(msg: WakeMessage, from: Host, sourceProto: Short, channelId: Int) {
        logger.info("$msg FROM $from. Sending to tree")
        if (state == State.INACTIVE)
            triggerNotification(ActivateNotification(msg.contact))
        else
            logger.warn("Received wake message while not dormant, ignoring")
    }

    private fun onTreeBuilderTimer(timer: TreeBuilderTimer, timerId: Long) {

        when (treeBuilder) {
            TreeBuilder.Random -> {
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

            TreeBuilder.Static -> {
                if (state == State.INACTIVE)
                    return
                if (staticTree!!.containsKey(config.hostname)) {
                    for (host in staticTree[config.hostname]!!) {
                        val toWake = Host(Inet4Address.getByName(host), PORT)
                        if (membership.containsKey(toWake.address)
                            && membership[toWake.address]!!.first.state == State.INACTIVE
                        ) {
                            logger.info("Waking up $toWake")
                            openConnection(toWake)
                            sendMessage(WakeMessage(selfAddress), toWake)
                        }
                    }
                }
            }

            TreeBuilder.Location -> {
                if (state == State.ACTIVE)
                    return

                if (membership.size != config.tree_builder_nnodes - 1) {
                    logger.info("Waiting for all nodes to be in membership: ${membership.size}/${config.tree_builder_nnodes - 1}")
                    logger.debug("{}", membership.keys.map { it.hostAddress })
                    return
                }
                val best =
                    membership.filterValues { it.first.location.distanceToCenter() < myLocation.distanceToCenter() }
                        .toList().minByOrNull { (_, value) -> locationSub.orderingFunc(value.first, myLocation, membership.size) }
                if (best != null) {
                    if (best.second.first.state == State.ACTIVE) {
                        logger.info("Waking myself. Connecting to ${best.first}, membership size ${membership.size}")
                        triggerNotification(ActivateNotification(best.first))
                        cancelTimer(treeBuilderTimer)
                    } else {
                        logger.info("Waiting for ${best.first} to wake up, membership size ${membership.size}")
                    }
                }

            }
        }
    }

    private fun onBroadcastTimer(timer: BroadcastTimer, timerId: Long) {
        sendRequest(BroadcastRequest(BroadcastState(selfAddress, myLocation, resources, state)), HyParFlood.ID)
        val time = getTimeMillis()
        membership.filterValues { it.second < time }.forEach {
            membership.remove(it.key)
            logger.debug("MEMBERSHIP remove {}", it.key)
        }
    }

    private fun onBroadcastReply(reply: BroadcastReply, from: Short) {
        val newState = BroadcastState.fromByteArray(reply.payload)
        val newExpiry = getTimeMillis() + membershipExpiration

        val existingPair = membership[newState.address]

        membership[newState.address] = Pair(newState, newExpiry)
        if (existingPair == null || newState != existingPair.first) {
            logger.debug("MEMBERSHIP update {} {}", newState, newExpiry)
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
        logger.debug("Connected out to {}", event.node)
    }

    private fun onOutConnectionFailed(event: OutConnectionFailed<ProtoMessage>, channelId: Int) {
        logger.warn("Failed connecting out to ${event.node}: ${event.cause.localizedMessage}")
    }

    private fun onOutConnectionDown(event: OutConnectionDown, channelId: Int) {
        logger.warn("Lost connection out to ${event.node}")
    }

    private fun onInConnectionUp(event: InConnectionUp, channelId: Int) {
        logger.debug("Connection in up from {}", event.node)
    }

    private fun onInConnectionDown(event: InConnectionDown, channelId: Int) {
        logger.debug("Connection in down from {}", event.node)
    }

    data class Location(val x: Double, val y: Double) {
        fun distanceToCenter(): Double {
            return sqrt(x.pow(2) + y.pow(2))
        }

        fun distanceTo(other: Location): Double {
            return sqrt((x - other.x).pow(2) + (y - other.y).pow(2))
        }

        fun angle(): Double {
            return (Math.toDegrees(Math.atan2(x, -y)) + 360.0) % 360.0
        }
    }
}