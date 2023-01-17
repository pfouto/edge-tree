import hyparflood.HyParFlood
import manager.Manager
import org.apache.logging.log4j.LogManager
import org.apache.logging.log4j.Logger
import pt.unl.fct.di.novasys.babel.core.Babel
import tree.Tree
import java.net.Inet4Address
import java.net.InetAddress
import java.net.NetworkInterface
import java.security.InvalidParameterException
import java.util.*

fun main(args: Array<String>) {
    System.setProperty("log4j2.configurationFile", "log4j2.xml")

    val logger: Logger = LogManager.getLogger()

    try {


        logger.info("Program arguments: ${args.joinToString()}")

        val properties = Babel.loadConfig(args, "properties.conf")
        addInterfaceIp(properties)

        val me: Inet4Address = Inet4Address.getByName(properties.getProperty("address")) as Inet4Address

        logger.info("Hello I am ${properties.getProperty("hostname")} ${me.hostAddress}")

        val babel = Babel.getInstance()

        val tree = Tree(me, properties)
        val hyParFlood = HyParFlood(me, properties)
        val manager = Manager(me, properties)

        babel.registerProtocol(tree)
        babel.registerProtocol(hyParFlood)
        babel.registerProtocol(manager)

        tree.init(properties)
        hyParFlood.init(properties)
        manager.init(properties)

        babel.start()

        Runtime.getRuntime().addShutdownHook(Thread { logger.info("Goodbye") })
    } catch (e: Exception) {
        logger.error("Exception caught in main: ${e.localizedMessage}", e)
    }

}

fun getIpOfInterface(interfaceName: String?): String? {
    val inetAddress = NetworkInterface.getByName(interfaceName).inetAddresses
    var currentAddress: InetAddress
    while (inetAddress.hasMoreElements()) {
        currentAddress = inetAddress.nextElement()
        if (currentAddress is Inet4Address && !currentAddress.isLoopbackAddress())
            return currentAddress.getHostAddress()
    }
    return null
}

fun addInterfaceIp(props: Properties) {
    val interfaceName: String? = props.getProperty("interface")
    if (interfaceName != null) {
        val ip = getIpOfInterface(interfaceName)
        if (ip != null) props.setProperty("address", ip)
        else throw InvalidParameterException("Property interface is set to $interfaceName, but has no ip")
    }
}