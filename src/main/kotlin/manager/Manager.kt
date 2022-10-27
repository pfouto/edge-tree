package manager

import manager.messaging.WakeMessage
import org.apache.logging.log4j.LogManager
import pt.unl.fct.di.novasys.babel.core.GenericProtocol
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage
import pt.unl.fct.di.novasys.channel.tcp.TCPChannel
import pt.unl.fct.di.novasys.channel.tcp.events.*
import pt.unl.fct.di.novasys.network.data.Host
import tree.BootstrapNotification
import tree.Tree
import java.io.File
import java.net.Inet4Address
import java.net.InetAddress
import java.util.*
import kotlin.random.Random

class Manager(address: Inet4Address, props: Properties) : GenericProtocol(NAME, ID) {

    companion object {
        const val NAME = "Manager"
        const val ID: Short = 1
        const val PORT = 2900
        const val POOL_FILE_KEY = "pool_file"
        const val POOL_FILE_DEFAULT = "pool.conf"

        private val logger = LogManager.getLogger()
    }

    private val self: Host
    private val channel: Int
    private val nodePool: MutableList<Host>
    private val clamp: Int

    init {
        self = Host(address, PORT)
        val channelProps = Properties()
        channelProps.setProperty(TCPChannel.ADDRESS_KEY, self.address.hostAddress)
        channelProps.setProperty(TCPChannel.PORT_KEY, self.port.toString())
        channelProps.setProperty(TCPChannel.TRIGGER_SENT_KEY, "true")
        channel = createChannel(TCPChannel.NAME, channelProps)

        val fileName = props.getProperty(POOL_FILE_KEY, POOL_FILE_DEFAULT)

        clamp = props.getProperty("pool_file_clamp").toInt()

        nodePool = mutableListOf()
        File(fileName).forEachLine {
            val split = it.split(":")
            val host = Host(InetAddress.getByName(split[0]), split[1].toInt())
            if (host != self)
                nodePool.add(host)
        }
        logger.info("Loaded node pool: $nodePool")

    }

    override fun init(props: Properties) {
        registerMessageSerializer(channel, WakeMessage.ID, WakeMessage.serializer)
        registerMessageHandler(channel, WakeMessage.ID, this::onWakeMessage, this::onWakeSent, this::onMessageFailed)

        registerChannelEventHandler(channel, OutConnectionFailed.EVENT_ID, this::onOutConnectionFailed)
        registerChannelEventHandler(channel, OutConnectionDown.EVENT_ID, this::onOutConnectionDown)
        registerChannelEventHandler(channel, InConnectionDown.EVENT_ID, this::onInConnectionDown)
        registerChannelEventHandler(channel, OutConnectionUp.EVENT_ID, this::onOutConnectionUp)
        registerChannelEventHandler(channel, InConnectionUp.EVENT_ID, this::onInConnectionUp)

        registerRequestHandler(ChildRequest.ID, this::onChildRequest)

        if(props.getProperty("bootstrap").toBoolean()){
            triggerNotification(BootstrapNotification(null))
        }

        logger.info("Bind address $self")
    }

    private fun onChildRequest(request: ChildRequest, from: Short){
        val idx = Random.nextInt(0, clamp-1)
        openConnection(nodePool[idx])
        sendMessage(WakeMessage(Host(self.address, Tree.PORT)), nodePool[idx])
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