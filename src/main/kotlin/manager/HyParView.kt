package manager

import manager.messaging.*
import org.apache.logging.log4j.LogManager
import pt.unl.fct.di.novasys.babel.core.GenericProtocol
import pt.unl.fct.di.novasys.network.data.Host
import java.net.Inet4Address
import java.util.*
import manager.utils.IView
import manager.utils.View
import pt.unl.fct.di.novasys.channel.tcp.TCPChannel
import pt.unl.fct.di.novasys.channel.tcp.events.*


class HyParView(address: Inet4Address, properties: Properties) : GenericProtocol(NAME, ID) {
    companion object {
        const val NAME = "HyParView"
        const val ID: Short = 101
        const val PORT = 2900

        const val MAX_BACKOFF: Int = 60000
        private val logger = LogManager.getLogger()
    }

    private val myself: Host

    private val ARWL: Short //param: active random walk length
    private val PRWL: Short //param: passive random walk length
    private val shuffleTime: Short //param: timeout for shuffle
    private val originalTimeout: Short //param: timeout for hello messages
    private val timeout: Short //param: timeout for hello msgs
    private val joinTimeout: Short //param: timeout for retry join
    private val kActive: Short //param: number of active nodes to exchange on shuffle
    private val kPassive: Short //param: number of passive nodes to exchange on shuffle

    private var active: IView
    private var passive: IView

    private var pending: Set<Host>
    private val activeShuffles: Map<Short, Array<Host>>

    private val seqNum: Short = 0

    private val rnd: Random

    init {
        myself = Host(address, PORT)

        ARWL = properties.getProperty("ARWL", "4").toShort()
        PRWL = properties.getProperty("PRWL", "2").toShort()

        shuffleTime = properties.getProperty("shuffleTime", "2000").toShort()
        timeout  = properties.getProperty("helloBackoff", "1000").toShort()
        originalTimeout = timeout

        joinTimeout = properties.getProperty("joinTimeout", "2000").toShort()
        kActive = properties.getProperty("kActive", "2").toShort()
        kPassive = properties.getProperty("kPassive", "3").toShort()

        rnd = Random()
        val maxActive: Int = properties.getProperty("ActiveView", "4").toInt()
        val maxPassive: Int = properties.getProperty("PassiveView", "7").toInt()
        active = View(true, maxActive, myself, rnd)
        passive = View(false, maxPassive, myself, rnd)

        pending = HashSet()
        activeShuffles = TreeMap()

        active.setOther(passive, pending)
        passive.setOther(active, pending)

        val channelProps = Properties()
        channelProps.setProperty(TCPChannel.ADDRESS_KEY, properties.getProperty("address")) // The address to bind to
        channelProps.setProperty(TCPChannel.PORT_KEY, properties.getProperty("port")) // The port to bind to
        channelProps.setProperty(TCPChannel.TRIGGER_SENT_KEY, "true")
        val channelId = createChannel(TCPChannel.NAME, channelProps)

        /*---------------------- Register Message Serializers ---------------------- */
        registerMessageSerializer(channelId,JoinMessage.MSG_CODE,JoinMessage.serializer)
        registerMessageSerializer(channelId, JoinReplyMessage.MSG_CODE, JoinReplyMessage.serializer)
        registerMessageSerializer(channelId, ForwardJoinMessage.MSG_CODE, ForwardJoinMessage.serializer)
        registerMessageSerializer(channelId, HelloMessage.MSG_CODE, HelloMessage.serializer)
        registerMessageSerializer(channelId, HelloReplyMessage.MSG_CODE, HelloReplyMessage.serializer)
        registerMessageSerializer(channelId, DisconnectMessage.MSG_CODE, DisconnectMessage.serializer)
        registerMessageSerializer(channelId, ShuffleMessage.MSG_CODE, ShuffleMessage.serializer)
        registerMessageSerializer(channelId, ShuffleReplyMessage.MSG_CODE, ShuffleReplyMessage.serializer)

        /*---------------------- Register Message Handlers -------------------------- */
        registerMessageHandler(channelId, JoinMessage.MSG_CODE,this::uponReceiveJoin)
        registerMessageHandler(channelId, JoinReplyMessage.MSG_CODE, this::uponReceiveJoinReply)
        registerMessageHandler(channelId, ForwardJoinMessage.MSG_CODE, this::uponReceiveForwardJoin)
        registerMessageHandler(channelId, HelloMessage.MSG_CODE, this::uponReceiveHello)
        registerMessageHandler(channelId, HelloReplyMessage.MSG_CODE, this::uponReceiveHelloReply)
        registerMessageHandler(channelId, DisconnectMessage.MSG_CODE, this::uponReceiveDisconnect, this::uponDisconnectSent)
        registerMessageHandler(channelId, ShuffleMessage.MSG_CODE, this::uponReceiveShuffle)
        registerMessageHandler(channelId, ShuffleReplyMessage.MSG_CODE, this::uponReceiveShuffleReply, this::uponShuffleReplySent)

        /*--------------------- Register Timer Handlers ----------------------------- */
        registerTimerHandler(ShuffleTimeout.TIMER_ID, this::uponShuffleTimeout)
        registerTimerHandler(HelloTimeout.TIMER_ID, this::uponHelloTimeout)
        registerTimerHandler(JoinTimeout.TIMER_ID, this::uponJoinTimeout)

        /*-------------------- Register Channel Event ------------------------------- */
        registerChannelEventHandler(channelId, OutConnectionDown.EVENT_ID, this::uponOutConnectionDown)
        registerChannelEventHandler(channelId, OutConnectionFailed.EVENT_ID, this::uponOutConnectionFailed)
        registerChannelEventHandler(channelId, OutConnectionUp.EVENT_ID, this::uponOutConnectionUp)
        registerChannelEventHandler(channelId, InConnectionUp.EVENT_ID, this::uponInConnectionUp)
        registerChannelEventHandler(channelId, InConnectionDown.EVENT_ID, this::uponInConnectionDown)
    }

    override fun init(props: Properties?) {
        TODO("Not yet implemented")
    }

}