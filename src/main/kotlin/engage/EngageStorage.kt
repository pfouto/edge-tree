package engage

import Config
import engage.messaging.MetadataFlush
import engage.messaging.UpdateNot
import ipc.*
import org.apache.logging.log4j.LogManager
import pt.unl.fct.di.novasys.babel.core.GenericProtocol
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage
import java.net.Inet4Address
import java.net.InetAddress
import java.util.*
import kotlin.contracts.contract
import kotlin.system.exitProcess


//TODO periodically compute ops per second (or on shutdown?)
//This probably should start when the first write is received and stop when the last write is received


class EngageStorage(private val myAddress: Inet4Address, val config: Config) : GenericProtocol(NAME, ID) {
    companion object {
        const val NAME = "EngageStorage"
        const val ID: Short = 500

        private val logger = LogManager.getLogger()
    }

    private val lww: Int = myAddress.hashCode()

    private val storageWrapper: EngageInMemoryWrapper

    private var storageIdCounter = 0

    private val partitions: List<String> = config.engPartitions.split(",")

    private val pendingData: MutableMap<InetAddress, Queue<ProtoMessage>>

    private val globalClock: MutableMap<InetAddress, MutableInteger>

    private var amDc: Boolean = false

    private var nUpdates: Long = 0
    private var printed: Boolean = false
    private var zeroTime: Long = 0

    init {

        subscribeNotification(DeactivateNotification.ID) { _: DeactivateNotification, _ -> onDeactivate() }
        subscribeNotification(ActivateNotification.ID) { not: ActivateNotification, _ -> onActivate(not) }

        registerRequestHandler(EngageOpRequest.ID) { req: EngageOpRequest, _ -> onLocalOpRequest(req) }

        registerReplyHandler(UpdateNotReply.ID) { rep: UpdateNotReply, _ -> onRemoteUpdateNot(rep.update) }
        registerReplyHandler(MFReply.ID) { rep: MFReply, _ -> onMetadataFlush(rep.mf) }

        storageWrapper = EngageInMemoryWrapper()
        storageWrapper.initialize()

        pendingData = mutableMapOf()
        globalClock = mutableMapOf()
    }

    override fun init(props: Properties) {

    }

    private fun onActivate(notification: ActivateNotification) {
        amDc = notification.contact == null
        zeroTime = System.currentTimeMillis()

    }

    private fun onLocalOpRequest(req: EngageOpRequest) {

        when (req.op) {
            is WriteOperation -> {
                val objId = ObjectIdentifier(req.op.partition, req.op.key)

                val objectClock = Clock()
                objectClock.merge(req.op.clock)
                val currentClock = storageWrapper.getMetadata(objId)?.vc ?: Clock()
                objectClock.merge(currentClock)

                val vUp = storageIdCounter++

                val updateNot = UpdateNot(myAddress, vUp, req.op.partition, req.op.key, objectClock, req.op.value, null)
                sendRequest(UpdateNotRequest(updateNot), Engage.ID)

                execute(updateNot)

                val clientClock = Clock()
                clientClock.merge(objectClock)
                clientClock.merge(myAddress, vUp)

                sendReply(EngageOpReply(req.proxyId, clientClock, null), EngageClientProxy.ID)
            }

            is ReadOperation -> {
                assertOrExit(req.op.partition in partitions, "Invalid partition in read op ${req.op.partition}")
                val objId = ObjectIdentifier(req.op.partition, req.op.key)
                when (val data = storageWrapper.get(objId)) {
                    null -> sendReply(EngageOpReply(req.proxyId, null, null), EngageClientProxy.ID)
                    else -> sendReply(EngageOpReply(req.proxyId, data.metadata.vc, data.value), EngageClientProxy.ID)
                }
            }
        }
    }

    private fun execute(not: UpdateNot) {
        val source = not.source
        val vUp = not.vUp
        val gPos = globalClock.computeIfAbsent(source) { MutableInteger() }
        assertOrExit(vUp == gPos.getValue() + 1, "Invalid vUp $vUp from $source")
        storageWrapper.put(ObjectIdentifier(not.part, not.key), ObjectData(not.data, ObjectMetadata(not.vc, lww)))

        val now = System.currentTimeMillis() - zeroTime
        if(amDc && config.count_ops && now > config.count_ops_start && !printed){
            if(now > config.count_ops_end){
                printed = true
                logger.info("Total updates: $nUpdates from ${config.count_ops_start} to ${config.count_ops_end}")
            } else {
                nUpdates++
            }
        }

        gPos.setValue(vUp)
    }

    private fun onRemoteUpdateNot(update: UpdateNot) {
        if (update.mf != null)
            onMetadataFlush(update.mf)

        val source = update.source
        pendingData.computeIfAbsent(source) { LinkedList() }.let {
            it.add(update)
            if (it.size == 1) tryExecQueue(source) //If previous op in queue, then no point trying anything
        }
    }

    private fun onMetadataFlush(mf: MetadataFlush) {
        for (obj in mf.updates.keys) {
            pendingData.computeIfAbsent(obj) { LinkedList() }.let {
                it.add(mf)
                if (it.size == 1) tryExecQueue(obj) //Will always execute
            }
        }
    }

    private fun tryExecQueue(source: InetAddress) {
        val hostData = pendingData.computeIfAbsent(source) { LinkedList() }
        var executed = true
        var tryAll = false

        while (executed && hostData.isNotEmpty()) {
            val peek = hostData.peek()
            executed = false

            when (peek) {
                is MetadataFlush -> {
                    hostData.remove() //Can always execute metadata flush

                    val gPos = globalClock.computeIfAbsent(source) { MutableInteger() }
                    val newClockPos = peek.updates[source]!!
                    gPos.setValue(newClockPos)
                    executed = true
                    tryAll = true
                }

                is UpdateNot -> {
                    if (canExec(peek.vc)) {
                        hostData.remove()
                        execute(peek)
                        executed = true
                    }
                }
            }
        }
        if (tryAll)
            pendingData.keys.forEach(this::tryExecQueue)
    }


    private fun canExec(opClock: Clock): Boolean {
        for ((key, value) in opClock.getValue()) {
            if (key != myAddress && getClockValue(key).getValue() < value)
                return false
        }
        return true
    }

    private fun getClockValue(key: InetAddress): ImmutableInteger {
        return globalClock.computeIfAbsent(key) { MutableInteger() }
    }

    private fun onDeactivate() {
        logger.info("Storage Deactivating (cleaning data)")
        storageWrapper.cleanUp()
    }

    private fun assertOrExit(condition: Boolean, msg: String) {
        if (!condition) {
            logger.error(msg, AssertionError())
            exitProcess(1)
        }
    }

}