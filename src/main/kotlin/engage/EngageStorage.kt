package engage

import Config
import ipc.*
import org.apache.logging.log4j.LogManager
import pt.unl.fct.di.novasys.babel.core.GenericProtocol
import storage.StorageWrapper
import storage.utils.DataIndex
import tree.utils.PropagateTimer
import java.net.Inet4Address
import java.util.*

//TODO take logic from engage-cassandra

class EngageStorage(val address: Inet4Address, private val config: Config) : GenericProtocol(NAME, ID) {
    companion object {
        const val NAME = "EngageStorage"
        const val ID: Short = 500

        private val logger = LogManager.getLogger()
    }

    private val lww: Int = address.hashCode()

    private lateinit var storageWrapper: StorageWrapper

    private lateinit var dataIndex: DataIndex

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

        registerReplyHandler(PersistenceUpdate.ID) { rep: PersistenceUpdate, _ -> onPersistence(rep) }

        registerReplyHandler(MigrationReply.ID) { rep: MigrationReply, _ -> onMigrationReply(rep) }

        registerTimerHandler(PropagateTimer.ID) { _: PropagateTimer, _ -> propagateTime() }

        //TODO check if I am VC! (from onactivate notification)
        setupPeriodicTimer(PropagateTimer(), propagateTimeout, propagateTimeout)


    }

    override fun init(props: Properties) {

    }

    private fun onActivate(notification: ActivateNotification) {

    }

    fun propagateTime() {
        //TODO send notification with VC to Engage
        //TODO only if I am DC
    }


    private fun onDeactivate() {
        logger.info("Storage Deactivating (cleaning data)")
        dataIndex.clear()
        storageWrapper.cleanUp()
    }


}