package engage

import Config
import engage.messaging.MetadataFlush
import engage.messaging.UpdateNot
import ipc.*
import org.apache.logging.log4j.LogManager
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

    private var storageIdCounter = 0

    private val proxyMapper = mutableMapOf<Int, Long>()

    private val propagateTimeout: Long

    init {
        propagateTimeout = config.tree_propagate_timeout

        subscribeNotification(DeactivateNotification.ID) { _: DeactivateNotification, _ -> onDeactivate() }
        subscribeNotification(ActivateNotification.ID) { not: ActivateNotification, _ -> onActivate(not) }

        registerRequestHandler(EngageOpRequest.ID) { req: EngageOpRequest, _ -> onLocalOpRequest(req) }

        registerReplyHandler(UpdateNotReply.ID) { rep: UpdateNotReply, _ -> onRemoteUpdateNot(rep.update) }
        registerReplyHandler(MFReply.ID) { rep: MFReply, _ -> onMetadataFlush(rep.mf) }

        storageWrapper = EngageInMemoryWrapper()
        storageWrapper.initialize()
    }

    override fun init(props: Properties) {

    }

    private fun onActivate(notification: ActivateNotification) {

    }

    private fun onLocalOpRequest(req: EngageOpRequest) {
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

    private fun onRemoteUpdateNot(update: UpdateNot) {

    }

    private fun onMetadataFlush(mf: MetadataFlush) {

    }

    private fun onDeactivate() {
        logger.info("Storage Deactivating (cleaning data)")
        storageWrapper.cleanUp()
    }

}