package manager

import manager.utils.ChildRequest
import manager.utils.messaging.WakeMessage
import org.apache.logging.log4j.LogManager
import pt.unl.fct.di.novasys.babel.core.GenericProtocol
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage
import pt.unl.fct.di.novasys.channel.tcp.TCPChannel
import pt.unl.fct.di.novasys.channel.tcp.events.*
import pt.unl.fct.di.novasys.network.data.Host
import tree.utils.BootstrapNotification
import tree.Tree
import java.io.File
import java.net.Inet4Address
import java.net.InetAddress
import java.util.*
import kotlin.random.Random

class Manager(address: Inet4Address, props: Properties) : GenericProtocol(NAME, ID) {

    companion object {
        const val NAME = "Manager"
        const val ID: Short = 101
        const val PORT = 2900
        const val POOL_FOLDER_KEY = "pool_folder"

        private val logger = LogManager.getLogger()
    }

    private val self: Host
    private val channel: Int
    private val nodePool: MutableList<Host>

    private var bootstrap: Boolean
    private var region: String = ""

    init {
        self = Host(address, PORT)
        val channelProps = Properties()
        channelProps.setProperty(TCPChannel.ADDRESS_KEY, self.address.hostAddress)
        channelProps.setProperty(TCPChannel.PORT_KEY, self.port.toString())
        channelProps.setProperty(TCPChannel.TRIGGER_SENT_KEY, "true")
        channel = createChannel(TCPChannel.NAME, channelProps)

        val poolFolderName = props.getProperty(POOL_FOLDER_KEY)

        bootstrap = false
        var myFile = false
        nodePool = mutableListOf()

        for (file in File(poolFolderName).walk()) {
            if (file.isFile && file.extension == "pool") {
                var index = 0
                file.forEachLine {
                    val hostTokens = it.split(":")
                    val host = Host(InetAddress.getByName(hostTokens[0]), hostTokens[1].toInt())
                    if (host == self) {
                        myFile = true
                        region = file.nameWithoutExtension
                        if (index == 0)
                            bootstrap = true
                    } else
                        nodePool.add(host)
                    index++
                }
                if(myFile)
                    break
            }
            nodePool.clear()
        }

        if(region.isEmpty())
            throw IllegalArgumentException("No pool file found for this node")

        logger.info("Region $region, bootstrap $bootstrap, node pool: $nodePool")

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

        if (bootstrap)
            triggerNotification(BootstrapNotification(null))

        logger.info("Bind address $self")
    }

    private fun onChildRequest(request: ChildRequest, from: Short) {
        val idx = Random.nextInt(0, nodePool.size)
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