import java.util.*

class Config(properties: Properties) {

    companion object {
        // HyParFlood
        const val HPF_ARWL_KEY = "ARWL"
        const val HPF_ARWL_DEFAULT = "4"
        const val HPF_PRWL_KEY = "PRWL"
        const val HPF_PRWL_DEFAULT = "2"
        const val HPF_SHUFFLE_TIME_KEY = "shuffle_time"
        const val HPF_SHUFFLE_TIME_DEFAULT = "2000"
        const val HPF_HELLO_BACKOFF_KEY = "hello_backoff"
        const val HPF_HELLO_BACKOFF_DEFAULT = "1000"
        const val HPF_JOIN_TIMEOUT_KEY = "join_timeout"
        const val HPF_JOIN_TIMEOUT_DEFAULT = "2000"
        const val HPF_K_ACTIVE_KEY = "k_active"
        const val HPF_K_ACTIVE_DEFAULT = "2"
        const val HPF_K_PASSIVE_KEY = "k_passive"
        const val HPF_K_PASSIVE_DEFAULT = "3"
        const val HPF_ACTIVE_VIEW_KEY = "active_view"
        const val HPF_ACTIVE_VIEW_DEFAULT = "4"
        const val HPF_PASSIVE_VIEW_KEY = "passive_view"
        const val HPF_PASSIVE_VIEW_DEFAULT = "7"

        // Manager
        const val MAN_BROADCAST_INTERVAL_KEY = "broadcast_interval"
        const val MAN_BROADCAST_INTERVAL_DEFAULT = "2000"
        const val DATACENTER_KEY = "datacenter"
        const val REGION_KEY = "region"

        // General
        const val HOSTNAME_KEY = "hostname"
        const val IP_ADDR_KEY = "address"
        const val HPF_PORT_KEY = "hpf_port"
        const val HPF_PORT_DEFAULT = "2000"
        const val MAN_PORT_KEY = "man_port"
        const val MAN_PORT_DEFAULT = "2100"
        const val TREE_PORT_KEY = "tree_port"
        const val TREE_PORT_DEFAULT = "2200"
        const val CLIENT_PORT_KEY = "client_port"
        const val CLIENT_PORT_DEFAULT = "2300"
    }

    val hpf_arwl: Int
    val hpf_prwl: Int
    val hpf_shuffle_time: Int
    val hpf_hello_backoff: Int
    val hpf_join_timeout: Int
    val hpf_k_active: Int
    val hpf_k_passive: Int
    val hpf_active_view: Int
    val hpf_passive_view: Int

    val man_broadcast_interval: Int
    val datacenter: String
    val region: String

    val hostname: String
    val ip_addr: String

    val hpf_port: Int
    val man_port: Int
    val tree_port: Int
    val client_port: Int

    init {
        hpf_arwl = properties.getProperty(HPF_ARWL_KEY, HPF_ARWL_DEFAULT).toInt()
        hpf_prwl = properties.getProperty(HPF_PRWL_KEY, HPF_PRWL_DEFAULT).toInt()
        hpf_shuffle_time = properties.getProperty(HPF_SHUFFLE_TIME_KEY, HPF_SHUFFLE_TIME_DEFAULT).toInt()
        hpf_hello_backoff = properties.getProperty(HPF_HELLO_BACKOFF_KEY, HPF_HELLO_BACKOFF_DEFAULT).toInt()
        hpf_join_timeout = properties.getProperty(HPF_JOIN_TIMEOUT_KEY, HPF_JOIN_TIMEOUT_DEFAULT).toInt()
        hpf_k_active = properties.getProperty(HPF_K_ACTIVE_KEY, HPF_K_ACTIVE_DEFAULT).toInt()
        hpf_k_passive = properties.getProperty(HPF_K_PASSIVE_KEY, HPF_K_PASSIVE_DEFAULT).toInt()
        hpf_active_view = properties.getProperty(HPF_ACTIVE_VIEW_KEY, HPF_ACTIVE_VIEW_DEFAULT).toInt()
        hpf_passive_view = properties.getProperty(HPF_PASSIVE_VIEW_KEY, HPF_PASSIVE_VIEW_DEFAULT).toInt()

        man_broadcast_interval =
            properties.getProperty(MAN_BROADCAST_INTERVAL_KEY, MAN_BROADCAST_INTERVAL_DEFAULT).toInt()
        datacenter = properties.getProperty(DATACENTER_KEY)
        region = properties.getProperty(REGION_KEY)

        hostname = properties.getProperty(HOSTNAME_KEY)
        ip_addr = properties.getProperty(IP_ADDR_KEY)

        hpf_port = properties.getProperty(HPF_PORT_KEY, HPF_PORT_DEFAULT).toInt()
        man_port = properties.getProperty(MAN_PORT_KEY, MAN_PORT_DEFAULT).toInt()
        tree_port = properties.getProperty(TREE_PORT_KEY, TREE_PORT_DEFAULT).toInt()
        client_port = properties.getProperty(CLIENT_PORT_KEY, CLIENT_PORT_DEFAULT).toInt()
    }
}