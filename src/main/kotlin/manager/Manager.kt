package manager

import hyparview.HyParView
import hyparview.utils.BroadcastReply
import hyparview.utils.InitRequest
import manager.utils.BroadcastState
import manager.utils.BroadcastTimer
import manager.utils.ChildRequest
import manager.utils.messaging.WakeMessage
import org.apache.logging.log4j.LogManager
import pt.unl.fct.di.novasys.babel.core.GenericProtocol
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage
import pt.unl.fct.di.novasys.channel.tcp.TCPChannel
import pt.unl.fct.di.novasys.channel.tcp.events.*
import pt.unl.fct.di.novasys.network.data.Host
import tree.utils.BootstrapNotification
import java.net.Inet4Address
import java.util.*

class Manager(address: Inet4Address, props: Properties) : GenericProtocol(NAME, ID) {

    companion object {
        const val NAME = "Manager"
        const val ID: Short = 101
        const val PORT = 2900

        const val DATACENTER_KEY = "datacenter"
        const val REGION_KEY = "region"

        private val logger = LogManager.getLogger()
    }

    private val self: Host
    private val channel: Int

    private val region: String
    private val regionalDatacenter: String
    private val amDatacenter: Boolean

    private val membership: MutableMap<Host, BroadcastState>

    init {
        self = Host(address, PORT)
        val channelProps = Properties()
        channelProps.setProperty(TCPChannel.ADDRESS_KEY, self.address.hostAddress)
        channelProps.setProperty(TCPChannel.PORT_KEY, self.port.toString())
        channelProps.setProperty(TCPChannel.TRIGGER_SENT_KEY, "true")
        channel = createChannel(TCPChannel.NAME, channelProps)

        registerMessageSerializer(channel, WakeMessage.ID, WakeMessage.serializer)
        registerMessageHandler(channel, WakeMessage.ID, this::onWakeMessage, this::onWakeSent, this::onMessageFailed)

        registerChannelEventHandler(channel, OutConnectionFailed.EVENT_ID, this::onOutConnectionFailed)
        registerChannelEventHandler(channel, OutConnectionDown.EVENT_ID, this::onOutConnectionDown)
        registerChannelEventHandler(channel, InConnectionDown.EVENT_ID, this::onInConnectionDown)
        registerChannelEventHandler(channel, OutConnectionUp.EVENT_ID, this::onOutConnectionUp)
        registerChannelEventHandler(channel, InConnectionUp.EVENT_ID, this::onInConnectionUp)

        registerRequestHandler(ChildRequest.ID, this::onChildRequest)
        registerReplyHandler(BroadcastReply.ID, this::onBroadcastReply)

        registerTimerHandler(BroadcastTimer.TIMER_ID, this::onBroadcastTimer)

        region = props.getProperty(REGION_KEY)
        regionalDatacenter = props.getProperty(DATACENTER_KEY)
        amDatacenter = regionalDatacenter == props.getProperty("hostname")

        membership = mutableMapOf()

        logger.info("Region $region, amDc $amDatacenter, regionalDc: $regionalDatacenter")
    }

    override fun init(props: Properties) {

        if (amDatacenter) {
            triggerNotification(BootstrapNotification(null))
            sendRequest(InitRequest(null), HyParView.ID)
        } else
            sendRequest(InitRequest(Inet4Address.getByName(regionalDatacenter) as Inet4Address), HyParView.ID)

        logger.info("Bind address $self")
    }

    private fun onBroadcastTimer(timer: BroadcastTimer, timerId: Long) {

    }

    private fun onBroadcastReply(reply: BroadcastReply, from: Short) {

    }


    private fun onChildRequest(request: ChildRequest, from: Short) {
        //val idx = Random.nextInt(0, nodePool.size)
        //openConnection(nodePool[idx])
        //sendMessage(WakeMessage(Host(self.address, Tree.PORT)), nodePool[idx])
    }

    private fun onWakeMessage(msg: WakeMessage, from: Host, sourceProto: Short, channelId: Int) {
        logger.info("$msg FROM $from. Sending to tree")
        triggerNotification(BootstrapNotification(msg.contact))
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