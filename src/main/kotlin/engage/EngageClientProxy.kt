package engage

import Config
import engage.messaging.RequestMessage
import engage.messaging.ResponseMessage
import ipc.ActivateNotification
import ipc.DeactivateNotification
import ipc.EngageOpReply
import ipc.EngageOpRequest
import org.apache.logging.log4j.LogManager
import pt.unl.fct.di.novasys.babel.core.GenericProtocol
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage
import pt.unl.fct.di.novasys.channel.simpleclientserver.SimpleServerChannel
import pt.unl.fct.di.novasys.channel.simpleclientserver.events.ClientDownEvent
import pt.unl.fct.di.novasys.channel.simpleclientserver.events.ClientUpEvent
import pt.unl.fct.di.novasys.network.data.Host
import java.net.Inet4Address
import java.util.*

class EngageClientProxy(address: Inet4Address, config: Config) : GenericProtocol(NAME, ID) {
    companion object {
        const val NAME = "EngageClientProxy"
        const val ID: Short = 400

        private const val PORT = 2400

        private val logger = LogManager.getLogger()
    }

    private var proxyIdCounter = 0L

    private val pendingOperations = mutableMapOf<Long, Pair<Host, RequestMessage>>()

    private val self: Host

    init {
        self = Host(address, PORT)
        val channelProps = Properties()
        channelProps.setProperty(SimpleServerChannel.ADDRESS_KEY, address.hostAddress)
        channelProps.setProperty(SimpleServerChannel.PORT_KEY, self.port.toString())
        channelProps.setProperty(SimpleServerChannel.HEARTBEAT_INTERVAL_KEY, "0")
        channelProps.setProperty(SimpleServerChannel.HEARTBEAT_TOLERANCE_KEY, "0")
        val channel = createChannel(SimpleServerChannel.NAME, channelProps)

        registerChannelEventHandler(channel, ClientUpEvent.EVENT_ID, this::onClientUp)
        registerChannelEventHandler(channel, ClientDownEvent.EVENT_ID, this::onClientDown)

        registerMessageSerializer(channel, RequestMessage.ID, RequestMessage.Serializer)
        registerMessageSerializer(channel, ResponseMessage.ID, ResponseMessage.Serializer)

        registerMessageHandler(
            channel, RequestMessage.ID,
            { msg: RequestMessage, from: Host, _, _ -> onRequestMessage(from, msg) },
            { msg: RequestMessage, to: Host, _, cause: Throwable, _ -> onMessageFailed(msg, to, cause) }
        )

        subscribeNotification(DeactivateNotification.ID) { _: DeactivateNotification, _ -> onDeactivate() }
        subscribeNotification(ActivateNotification.ID) { not: ActivateNotification, _ -> onActivate(not) }

        registerReplyHandler(EngageOpReply.ID) { rep: EngageOpReply, _ -> onOpReply(rep) }
    }


    override fun init(props: Properties) {

    }

    private fun onActivate(notification: ActivateNotification) {
        logger.info("ClientProxy Activating")
    }

    private fun onDeactivate() {
        logger.info("ClientProxy Deactivating")
    }

    private fun onRequestMessage(from: Host, msg: RequestMessage) {
        val proxyId = proxyIdCounter++
        val pair = Pair(from, msg)
        logger.debug("ID-MAPPING client {} {} proxy {}", from, msg.id, proxyId)
        pendingOperations[proxyId] = pair
        sendRequest(EngageOpRequest(proxyId, msg.op), EngageStorage.ID)
    }

    private fun onOpReply(reply: EngageOpReply) {
        val pair = pendingOperations.remove(reply.proxyId)
            ?: throw IllegalStateException("Received reply for unknown operation ${reply.proxyId}")
        sendMessage(ResponseMessage(pair.second.id, reply.clock, reply.data), pair.first)
    }

    private fun onClientUp(event: ClientUpEvent, channel: Int) {
        logger.info("Client connected " + event.client)
    }

    private fun onClientDown(event: ClientDownEvent, channel: Int) {
        logger.info("Client disconnected " + event.client)
    }

    private fun onMessageFailed(msg: ProtoMessage, to: Host, cause: Throwable) {
        logger.warn("Message $msg to $to failed: ${cause.localizedMessage}")
    }


}