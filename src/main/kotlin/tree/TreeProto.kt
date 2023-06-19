package tree

import Config
import ipc.*
import org.apache.logging.log4j.LogManager
import pt.unl.fct.di.novasys.babel.core.GenericProtocol
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage
import pt.unl.fct.di.novasys.channel.tcp.TCPChannel
import pt.unl.fct.di.novasys.channel.tcp.events.*
import pt.unl.fct.di.novasys.network.data.Host
import tree.messaging.down.*
import tree.utils.PropagateTimer
import tree.utils.ReconnectTimer
import tree.messaging.up.*
import java.net.Inet4Address
import java.util.*

abstract class TreeProto(val address: Inet4Address, config: Config) : GenericProtocol(NAME, ID) {

    companion object {
        const val NAME = "Tree"
        const val ID: Short = 200

        const val PORT = 2200

        private val logger = LogManager.getLogger()
    }

    private val reconnectTimeout: Long
    private val propagateTimeout: Long

    private val channel: Int

    val self: Host

    init {
        reconnectTimeout = config.tree_reconnect_timeout
        propagateTimeout = config.tree_propagate_timeout

        self = Host(address, PORT)
        val channelProps = Properties()
        channelProps.setProperty(TCPChannel.ADDRESS_KEY, address.hostAddress)
        channelProps.setProperty(TCPChannel.PORT_KEY, PORT.toString())
        channelProps.setProperty(TCPChannel.TRIGGER_SENT_KEY, "true")
        channel = createChannel(TCPChannel.NAME, channelProps)

        registerChannelEventHandler(channel, OutConnectionUp.EVENT_ID)
        { event: OutConnectionUp, _ -> parentConnected(event.node) }

        registerChannelEventHandler(channel, OutConnectionFailed.EVENT_ID)
        { event: OutConnectionFailed<ProtoMessage>, _ -> parentConnectionFailed(event.node, event.cause) }

        registerChannelEventHandler(channel, OutConnectionDown.EVENT_ID)
        { event: OutConnectionDown, _ -> parentConnectionLost(event.node, event.cause) }

        registerChannelEventHandler(channel, InConnectionUp.EVENT_ID)
        { event: InConnectionUp, _: Int -> onChildConnected(event.node) }

        registerChannelEventHandler(channel, InConnectionDown.EVENT_ID)
        { event: InConnectionDown, _: Int -> onChildDisconnected(event.node) }

        registerMessageSerializer(channel, SyncRequest.ID, SyncRequest.Serializer)
        registerMessageSerializer(channel, SyncResponse.ID, SyncResponse.Serializer)
        registerMessageSerializer(channel, UpstreamMetadata.ID, UpstreamMetadata.Serializer)
        registerMessageSerializer(channel, DownstreamMetadata.ID, DownstreamMetadata.Serializer)
        registerMessageSerializer(channel, Reject.ID, Reject.Serializer)
        registerMessageSerializer(channel, Reconfiguration.ID, Reconfiguration.Serializer)
        registerMessageSerializer(channel, ObjectReplicationRequest.ID, ObjectReplicationRequest.Serializer)
        registerMessageSerializer(channel, ObjectReplicationReply.ID, ObjectReplicationReply.Serializer)
        registerMessageSerializer(channel, PartitionReplicationRequest.ID, PartitionReplicationRequest.Serializer)
        registerMessageSerializer(channel, PartitionReplicationReply.ID, PartitionReplicationReply.Serializer)
        registerMessageSerializer(channel, UpstreamWrite.ID, UpstreamWrite.Serializer)
        registerMessageSerializer(channel, DownstreamWrite.ID, DownstreamWrite.Serializer)
        registerMessageSerializer(channel, ReplicaRemovalRequest.ID, ReplicaRemovalRequest.Serializer)

        registerMessageHandler(
            channel,
            SyncRequest.ID,
            { msg: SyncRequest, from: Host, _, _ -> onChildSyncRequest(from, msg) },
            { msg: SyncRequest, to: Host, _, cause: Throwable, _ -> onMessageFailed(msg, to, cause) }
        )

        registerMessageHandler(
            channel, SyncResponse.ID,
            { msg: SyncResponse, from, _, _ -> onParentSyncResponse(from, msg) },
            { msg: SyncResponse, to: Host, _, cause: Throwable, _ -> onMessageFailed(msg, to, cause) }
        )
        registerMessageHandler(
            channel, UpstreamMetadata.ID,
            { msg: UpstreamMetadata, from, _, _ -> onChildUpstreamMetadata(from, msg) },
            { msg: UpstreamMetadata, to: Host, _, cause: Throwable, _ -> onMessageFailed(msg, to, cause) }
        )

        registerMessageHandler(
            channel, DownstreamMetadata.ID,
            { msg: DownstreamMetadata, from, _, _ -> onParentDownstreamMetadata(from, msg) },
            { msg: DownstreamMetadata, to: Host, _, cause: Throwable, _ -> onMessageFailed(msg, to, cause) }
        )

        registerMessageHandler(
            channel, Reject.ID,
            { _, from, _, _ -> onReject(from) },
            { msg: Reject, to: Host, _, cause: Throwable, _ -> onMessageFailed(msg, to, cause) }
        )

        registerMessageHandler(
            channel, Reconfiguration.ID,
            { msg: Reconfiguration, from, _, _ -> onReconfiguration(from, msg) },
            { msg: Reconfiguration, to: Host, _, cause: Throwable, _ -> onMessageFailed(msg, to, cause) }
        )

        registerMessageHandler(
            channel, ObjectReplicationRequest.ID,
            { msg: ObjectReplicationRequest, from, _, _ -> onChildObjReplicationRequest(from, msg) },
            { msg: ObjectReplicationRequest, to: Host, _, cause: Throwable, _ -> onMessageFailed(msg, to, cause) }
        )
        registerMessageHandler(
            channel, ObjectReplicationReply.ID,
            { msg: ObjectReplicationReply, from, _, _ -> onParentObjReplicationReply(from, msg) },
            { msg: ObjectReplicationReply, to: Host, _, cause: Throwable, _ -> onMessageFailed(msg, to, cause) }
        )
        registerMessageHandler(
            channel, PartitionReplicationRequest.ID,
            { msg: PartitionReplicationRequest, from, _, _ -> onChildPartitionReplicationRequest(from, msg) },
            { msg: PartitionReplicationRequest, to: Host, _, cause: Throwable, _ -> onMessageFailed(msg, to, cause) }
        )
        registerMessageHandler(
            channel, PartitionReplicationReply.ID,
            { msg: PartitionReplicationReply, from, _, _ -> onParentPartitionReplicationReply(from, msg) },
            { msg: PartitionReplicationReply, to: Host, _, cause: Throwable, _ -> onMessageFailed(msg, to, cause) }
        )
        registerMessageHandler(
            channel, UpstreamWrite.ID,
            { msg: UpstreamWrite, from, _, _ -> onUpstreamWrite(from, msg) },
            { msg: UpstreamWrite, to: Host, _, cause: Throwable, _ -> onMessageFailed(msg, to, cause) }
        )
        registerMessageHandler(
            channel, DownstreamWrite.ID,
            { msg: DownstreamWrite, from, _, _ -> onDownstreamWrite(from, msg) },
            { msg: DownstreamWrite, to: Host, _, cause: Throwable, _ -> onMessageFailed(msg, to, cause) }
        )
        registerMessageHandler(
            channel, ReplicaRemovalRequest.ID,
            { msg: ReplicaRemovalRequest, from, _, _ -> onReplicaRemoval(from, msg) },
            { msg: ReplicaRemovalRequest, to: Host, _, cause: Throwable, _ -> onMessageFailed(msg, to, cause) }
        )


        subscribeNotification(ActivateNotification.ID) { not: ActivateNotification, _ -> onActivate(not) }
        subscribeNotification(DeactivateNotification.ID) { _: DeactivateNotification, _ -> onDeactivate() }

        registerTimerHandler(ReconnectTimer.ID) { timer: ReconnectTimer, _ -> openConnection(timer.node) }
        registerTimerHandler(PropagateTimer.ID) { _: PropagateTimer, _ -> propagateTime() }

        registerRequestHandler(ObjReplicationReq.ID) { req: ObjReplicationReq, _ -> onObjectReplicationRequest(req) }
        registerRequestHandler(PartitionReplicationReq.ID) { req: PartitionReplicationReq, _ ->
            onPartitionReplicationRequest(req)
        }
        registerReplyHandler(FetchObjectsRep.ID) { reply: FetchObjectsRep, _ -> onFetchObjectsReply(reply) }
        registerReplyHandler(FetchPartitionRep.ID) { reply: FetchPartitionRep, _ -> onFetchPartitionReply(reply) }

        registerRequestHandler(PropagateWriteRequest.ID) { req: PropagateWriteRequest, _ -> onPropagateLocalWrite(req) }

        registerReplyHandler(FetchMetadataRep.ID) { req: FetchMetadataRep, _ -> onFetchMetadataReply(req) }
        registerReplyHandler(DataDiffReply.ID) { req: DataDiffReply, _ -> onDataDiffReply(req) }

        registerRequestHandler(RemoveReplicasRequest.ID) { req: RemoveReplicasRequest, _ -> onRemoveReplicas(req) }
        registerRequestHandler(MigrationRequest.ID) { req: MigrationRequest, _ -> onMigrationRequest(req) }

    }


    override fun init(props: Properties) {
        logger.debug("Bind address {}", address)

        setupPeriodicTimer(PropagateTimer(), propagateTimeout, propagateTimeout)
    }

    //Lifecycle events
    abstract fun onActivate(notification: ActivateNotification)
    abstract fun onDeactivate()

    //Network events
    abstract fun parentConnected(host: Host)
    abstract fun parentConnectionLost(host: Host, cause: Throwable?)
    abstract fun parentConnectionFailed(host: Host, cause: Throwable?)
    abstract fun onChildConnected(child: Host)
    abstract fun onChildDisconnected(child: Host)
    abstract fun onMessageFailed(msg: ProtoMessage, to: Host, cause: Throwable)

    //Messaging from parent
    abstract fun onParentSyncResponse(parent: Host, msg: SyncResponse)
    abstract fun onReconfiguration(host: Host, reconfiguration: Reconfiguration)
    abstract fun onReject(host: Host)
    abstract fun onParentDownstreamMetadata(host: Host, msg: DownstreamMetadata)
    abstract fun onParentObjReplicationReply(parent: Host, msg: ObjectReplicationReply)
    abstract fun onParentPartitionReplicationReply(from: Host, msg: PartitionReplicationReply)
    abstract fun onDownstreamWrite(from: Host, msg: DownstreamWrite)


    //Messaging from child
    abstract fun onChildUpstreamMetadata(child: Host, msg: UpstreamMetadata)
    abstract fun onChildObjReplicationRequest(child: Host, msg: ObjectReplicationRequest)
    abstract fun onChildPartitionReplicationRequest(from: Host, msg: PartitionReplicationRequest, )
    abstract fun onChildSyncRequest(child: Host, msg: SyncRequest)
    abstract fun onUpstreamWrite(child: Host, msg: UpstreamWrite)
    abstract fun onReplicaRemoval(child: Host, msg: ReplicaRemovalRequest)

    //Timers
    abstract fun propagateTime()

    //Storage connection
    abstract fun onObjectReplicationRequest(request: ObjReplicationReq)
    abstract fun onPartitionReplicationRequest(req: PartitionReplicationReq)
    abstract fun onFetchObjectsReply(reply: FetchObjectsRep)
    abstract fun onFetchPartitionReply(reply: FetchPartitionRep)
    abstract fun onFetchMetadataReply(reply: FetchMetadataRep)
    abstract fun onDataDiffReply(reply: DataDiffReply)
    abstract fun onRemoveReplicas(req: RemoveReplicasRequest)
    abstract fun onMigrationRequest(req: MigrationRequest)

    abstract fun onPropagateLocalWrite(req: PropagateWriteRequest)
}