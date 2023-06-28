package proxy

import Config
import ipc.*
import org.apache.logging.log4j.LogManager
import proxy.messaging.PersistenceMessage
import proxy.messaging.ReconfigurationMessage
import proxy.messaging.RequestMessage
import proxy.messaging.ResponseMessage
import proxy.utils.WriteOperation
import pt.unl.fct.di.novasys.babel.core.GenericProtocol
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage
import pt.unl.fct.di.novasys.channel.simpleclientserver.SimpleServerChannel
import pt.unl.fct.di.novasys.channel.simpleclientserver.events.ClientDownEvent
import pt.unl.fct.di.novasys.channel.simpleclientserver.events.ClientUpEvent
import pt.unl.fct.di.novasys.network.data.Host
import storage.Storage
import java.net.Inet4Address
import java.util.*

class ClientProxy(address: Inet4Address, config: Config) : GenericProtocol(NAME, ID) {
    companion object {
        const val NAME = "ClientProxy"
        const val ID: Short = 400

        private const val PORT = 2400

        private val logger = LogManager.getLogger()
    }

    private val clients: MutableSet<Host> = mutableSetOf()

    private var opCounter = 0L

    private val pendingOperations = mutableMapOf<Long, Pair<Host, RequestMessage>>()
    private val pendingPersistence = mutableMapOf<Long, Pair<Host, RequestMessage>>()

    private val self: Host

    private var currentReconfiguration : List<Host> = listOf()

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
        registerMessageSerializer(channel, PersistenceMessage.ID, PersistenceMessage.Serializer)
        registerMessageSerializer(channel, ReconfigurationMessage.ID, ReconfigurationMessage.Serializer)

        registerMessageHandler(
            channel,
            RequestMessage.ID,
            { msg: RequestMessage, from: Host, _, _ -> onRequestMessage(from, msg) },
            { msg: RequestMessage, to: Host, _, cause: Throwable, _ -> onMessageFailed(msg, to, cause) }
        )

        subscribeNotification(DeactivateNotification.ID) { _: DeactivateNotification, _ -> onDeactivate() }
        subscribeNotification(ActivateNotification.ID) { not: ActivateNotification, _ -> onActivate(not) }

        registerReplyHandler(OpReply.ID) { rep: OpReply, _ -> onOpReply(rep) }
        registerReplyHandler(ClientWritePersistent.ID) { rep: ClientWritePersistent, _ -> onClientWritePersistent(rep) }
        registerReplyHandler(TreeReconfigurationClients.ID) { rep: TreeReconfigurationClients, _ -> onTreeReconfiguration(rep) }
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
        val opId = opCounter++
        val pair = Pair(from, msg)
        logger.debug("Client {} {} is node {}", from, msg.id, opId)
        pendingOperations[opId] = pair
        if (msg.op is WriteOperation && msg.op.persistence > 0) {
            pendingPersistence[opId] = pair
        }
        sendRequest(OpRequest(opId, msg.op), Storage.ID)
    }

    private fun onOpReply(reply: OpReply) {
        val pair = pendingOperations.remove(reply.id)!!
        sendMessage(ResponseMessage(pair.second.id, reply.hlc, reply.data), pair.first)
    }

    private fun onClientWritePersistent(rep: ClientWritePersistent) {
        val pair = pendingPersistence.remove(rep.id)!!
        sendMessage(PersistenceMessage(pair.second.id), pair.first)
    }

    private fun onTreeReconfiguration(rep: TreeReconfigurationClients) {
        currentReconfiguration = rep.hosts
        clients.forEach {
            sendMessage(ReconfigurationMessage(rep.hosts), it)
        }
    }

    private fun onClientUp(event: ClientUpEvent, channel: Int) {
        logger.info("Client connected " + event.client)
        clients.add(event.client)
        sendMessage(ReconfigurationMessage(currentReconfiguration), event.client)
    }

    private fun onClientDown(event: ClientDownEvent, channel: Int) {
        logger.info("Client disconnected " + event.client + " " + event.cause)
        clients.remove(event.client)
    }

    private fun onMessageFailed(msg: ProtoMessage, to: Host, cause: Throwable) {
        logger.warn("Message $msg to $to failed: ${cause.localizedMessage}")
    }


}