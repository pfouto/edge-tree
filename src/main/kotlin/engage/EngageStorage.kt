package engage

import Config
import ipc.*
import org.apache.logging.log4j.LogManager
import proxy.utils.ReadOperation
import proxy.utils.WriteOperation
import pt.unl.fct.di.novasys.babel.core.GenericProtocol
import java.net.Inet4Address
import java.util.*

//TODO take logic from engage-cassandra

//TODO periodically compute ops per second (or on shutdown?)
//This probably should start when the first write is received and stop when the last write is received


class EngageStorage(val address: Inet4Address, private val config: Config) : GenericProtocol(NAME, ID) {
    companion object {
        const val NAME = "EngageStorage"
        const val ID: Short = 500

        private val logger = LogManager.getLogger()
    }

    private val lww: Int = address.hashCode()

    private val storageWrapper: EngageInMemoryWrapper

    private val localTimeLock = Object()

    private var storageIdCounter = 0

    private val proxyMapper = mutableMapOf<Int, Long>()

    private val pendingPersistence = mutableMapOf<Int, MutableList<PropagateWriteRequest>>()

    private val propagateTimeout: Long

    init {
        propagateTimeout = config.tree_propagate_timeout

        subscribeNotification(DeactivateNotification.ID) { _: DeactivateNotification, _ -> onDeactivate() }
        subscribeNotification(ActivateNotification.ID) { not: ActivateNotification, _ -> onActivate(not) }

        registerRequestHandler(OpRequest.ID) { req: OpRequest, _ -> onLocalOpRequest(req) }
        registerReplyHandler(PropagateWriteReply.ID) { rep: PropagateWriteReply, _ ->
            onRemoteWrite(rep.writeId, rep.write, rep.downstream)
        }

        storageWrapper = EngageInMemoryWrapper()
        storageWrapper.initialize()
    }

    override fun init(props: Properties) {

    }

    private fun onActivate(notification: ActivateNotification) {

    }

    private fun onLocalOpRequest(req: OpRequest) {
        when (req.op) {
            is WriteOperation -> {
                val objId = ObjectIdentifier(req.op.partition, req.op.key)
                //TODO change object metadata to receive object instead of timestamp
                //or just use a different class instead of object data
                //val objData = ObjectData(req.op.value, ObjectMetadata(Clock(), lww))
            }

            is ReadOperation -> {

            }
        }
    }

    private fun onDeactivate() {
        logger.info("Storage Deactivating (cleaning data)")
        storageWrapper.cleanUp()
    }

}