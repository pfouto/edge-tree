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
        const val HPF_JOIN_TIMEOUT_DEFAULT = "3000"
        const val HPF_K_ACTIVE_KEY = "k_active"
        const val HPF_K_ACTIVE_DEFAULT = "3"
        const val HPF_K_PASSIVE_KEY = "k_passive"
        const val HPF_K_PASSIVE_DEFAULT = "4"
        const val HPF_ACTIVE_VIEW_KEY = "active_view"
        const val HPF_ACTIVE_VIEW_DEFAULT = "5"
        const val HPF_PASSIVE_VIEW_KEY = "passive_view"
        const val HPF_PASSIVE_VIEW_DEFAULT = "10"

        // Manager
        const val MAN_BROADCAST_INTERVAL_KEY = "broadcast_interval"
        const val MAN_BROADCAST_INTERVAL_DEFAULT = "2000"
        const val DATACENTER_KEY = "datacenter"
        const val REGION_KEY = "region"
        const val LOCATION_X_KEY = "location_x"
        const val LOCATION_Y_KEY = "location_y"
        const val TREE_BUILDER_KEY = "tree_builder"
        const val TREE_BUILDER_DEFAULT = "Location"
        const val TREE_BUILDER_INTERVAL_KEY = "tree_builder_interval"
        const val TREE_BUILDER_INTERVAL_DEFAULT = "1000"
        const val TREE_BUILD_STATIC_LOCATION = "tree_location"
        const val TREE_BUILD_STATIC_LOCATION_DEFAULT = ""
        const val TREE_BUILDER_LOCATION_DELAY_KEY = "tree_builder_location_delay"
        const val TREE_BUILDER_LOCATION_DELAY_DEFAULT = "20000"
        const val TREE_BUILDER_NNODES_KEY = "tree_builder_nnodes"
        const val TREE_BUILDER_LOCATION_SUB_KEY = "tree_builder_location_sub"
        const val TREE_BUILDER_LOCATION_SUB_DEFAULT = "deep"

        //Tree
        const val TREE_RECONNECT_TIMEOUT_KEY = "reconnect_timeout"
        const val TREE_RECONNECT_TIMEOUT_DEFAULT = "3000"
        const val TREE_PROPAGATE_TIMEOUT_KEY = "propagate_timeout"
        const val TREE_PROPAGATE_TIMEOUT_DEFAULT = "50"

        // Storage
        const val DC_STORAGE_TYPE_KEY = "dc_storage_type"
        const val DC_STORAGE_TYPE_DEFAULT = "in_memory" // "in_memory" or "cassandra"
        const val NODE_STORAGE_TYPE_KEY = "node_storage_type"
        const val NODE_STORAGE_TYPE_DEFAULT = "in_memory"
        const val GC_PERIOD_KEY = "gc_period"
        const val GC_PERIOD_DEFAULT = "60000" // 1 min
        const val GC_TRESHOLD_KEY = "gc_threshold"
        const val GC_TRESHOLD_DEFAULT = "300000" //5 min
        const val LOG_N_OBJECTS_KEY = "log_n_objects"
        const val LOG_N_OBJECTS_DEFAULT = "0"
        const val LOG_VISIBILITY_KEY = "log_visibility"
        const val LOG_VISIBILITY_DEFAULT = "false"

        // General
        const val HOSTNAME_KEY = "hostname"
        const val IP_ADDR_KEY = "address"

        const val COUNT_OPS_KEY = "count_ops"
        const val COUNT_OPS_DEFAULT = "false"

        const val COUNT_OPS_START_KEY = "count_ops_start"
        const val COUNT_OPS_START_DEFAULT = "0"

        const val COUNT_OPS_END_KEY = "count_ops_end"
        const val COUNT_OPS_END_DEFAULT = "0"

        //Engage
        const val ENGAGE_KEY = "engage"
        const val ENGAGE_DEFAULT = "false"
        const val ENG_PARTITIONS_KEY = "eng_partitions"
        const val ENG_PARTITIONS_DEFAULT = ""


        /*const val HPF_PORT_KEY = "hpf_port"
        const val HPF_PORT_DEFAULT = "2000"
        const val MAN_PORT_KEY = "man_port"
        const val MAN_PORT_DEFAULT = "2100"
        const val TREE_PORT_KEY = "tree_port"
        const val TREE_PORT_DEFAULT = "2200"
        const val CLIENT_PORT_KEY = "client_port"
        const val CLIENT_PORT_DEFAULT = "2300"*/
    }


    //noinspection
    val hpf_arwl: Short
    val hpf_prwl: Short
    val hpf_shuffle_time: Short
    val hpf_hello_backoff: Short
    val hpf_join_timeout: Short
    val hpf_k_active: Short
    val hpf_k_passive: Short
    val hpf_active_view: Int
    val hpf_passive_view: Int

    val man_broadcast_interval: Long
    val datacenter: String
    val region: String
    val tree_builder: String
    val tree_builder_interval: Long
    val tree_build_static_location: String
    val tree_builder_location_delay: Long
    val tree_builder_nnodes: Int
    val tree_builder_location_sub: String


    val tree_reconnect_timeout: Long
    val tree_propagate_timeout: Long

    val dc_storage_type: String
    val node_storage_type: String
    val gc_period: Long
    val gc_threshold: Long
    val log_n_objects: Long
    val log_visibility: Boolean

    val hostname: String
    val ip_addr: String

    val locationX: Double
    val locationY: Double

    val count_ops: Boolean
    val count_ops_start: Long
    val count_ops_end: Long

    val engage: Boolean
    val engPartitions: String

    /*val hpf_port: Int
    val man_port: Int
    val tree_port: Int
    val client_port: Int*/

    init {
        hpf_arwl = properties.getProperty(HPF_ARWL_KEY, HPF_ARWL_DEFAULT).toShort()
        hpf_prwl = properties.getProperty(HPF_PRWL_KEY, HPF_PRWL_DEFAULT).toShort()
        hpf_shuffle_time = properties.getProperty(HPF_SHUFFLE_TIME_KEY, HPF_SHUFFLE_TIME_DEFAULT).toShort()
        hpf_hello_backoff = properties.getProperty(HPF_HELLO_BACKOFF_KEY, HPF_HELLO_BACKOFF_DEFAULT).toShort()
        hpf_join_timeout = properties.getProperty(HPF_JOIN_TIMEOUT_KEY, HPF_JOIN_TIMEOUT_DEFAULT).toShort()
        hpf_k_active = properties.getProperty(HPF_K_ACTIVE_KEY, HPF_K_ACTIVE_DEFAULT).toShort()
        hpf_k_passive = properties.getProperty(HPF_K_PASSIVE_KEY, HPF_K_PASSIVE_DEFAULT).toShort()
        hpf_active_view = properties.getProperty(HPF_ACTIVE_VIEW_KEY, HPF_ACTIVE_VIEW_DEFAULT).toInt()
        hpf_passive_view = properties.getProperty(HPF_PASSIVE_VIEW_KEY, HPF_PASSIVE_VIEW_DEFAULT).toInt()

        man_broadcast_interval =
            properties.getProperty(MAN_BROADCAST_INTERVAL_KEY, MAN_BROADCAST_INTERVAL_DEFAULT).toLong()
        datacenter = properties.getProperty(DATACENTER_KEY)
        region = properties.getProperty(REGION_KEY)
        locationX = properties.getProperty(LOCATION_X_KEY).toDouble()
        locationY = properties.getProperty(LOCATION_Y_KEY).toDouble()
        tree_builder_location_delay =
            properties.getProperty(TREE_BUILDER_LOCATION_DELAY_KEY, TREE_BUILDER_LOCATION_DELAY_DEFAULT).toLong()

        tree_builder = properties.getProperty(TREE_BUILDER_KEY, TREE_BUILDER_DEFAULT)
        tree_builder_interval =
            properties.getProperty(TREE_BUILDER_INTERVAL_KEY, TREE_BUILDER_INTERVAL_DEFAULT).toLong()
        tree_build_static_location =
            properties.getProperty(TREE_BUILD_STATIC_LOCATION, TREE_BUILD_STATIC_LOCATION_DEFAULT)
        tree_builder_nnodes = properties.getProperty(TREE_BUILDER_NNODES_KEY).toInt()

        tree_reconnect_timeout =
            properties.getProperty(TREE_RECONNECT_TIMEOUT_KEY, TREE_RECONNECT_TIMEOUT_DEFAULT).toLong()
        tree_propagate_timeout =
            properties.getProperty(TREE_PROPAGATE_TIMEOUT_KEY, TREE_PROPAGATE_TIMEOUT_DEFAULT).toLong()

        tree_builder_location_sub =
            properties.getProperty(TREE_BUILDER_LOCATION_SUB_KEY, TREE_BUILDER_LOCATION_SUB_DEFAULT)

        dc_storage_type = properties.getProperty(DC_STORAGE_TYPE_KEY, DC_STORAGE_TYPE_DEFAULT)
        node_storage_type = properties.getProperty(NODE_STORAGE_TYPE_KEY, NODE_STORAGE_TYPE_DEFAULT)
        gc_period = properties.getProperty(GC_PERIOD_KEY, GC_PERIOD_DEFAULT).toLong()
        gc_threshold = properties.getProperty(GC_TRESHOLD_KEY, GC_TRESHOLD_DEFAULT).toLong()
        log_n_objects = properties.getProperty(LOG_N_OBJECTS_KEY, LOG_N_OBJECTS_DEFAULT).toLong()
        log_visibility = properties.getProperty(LOG_VISIBILITY_KEY, LOG_VISIBILITY_DEFAULT).toBoolean()

        hostname = properties.getProperty(HOSTNAME_KEY)
        ip_addr = properties.getProperty(IP_ADDR_KEY)

        count_ops = properties.getProperty(COUNT_OPS_KEY, COUNT_OPS_DEFAULT).toBoolean()
        count_ops_start = properties.getProperty(COUNT_OPS_START_KEY, COUNT_OPS_START_DEFAULT).toLong()
        count_ops_end = properties.getProperty(COUNT_OPS_END_KEY, COUNT_OPS_END_DEFAULT).toLong()

        engage = properties.getProperty(ENGAGE_KEY, ENGAGE_DEFAULT).toBoolean()
        engPartitions = properties.getProperty(ENG_PARTITIONS_KEY, ENG_PARTITIONS_DEFAULT)

        /*hpf_port = properties.getProperty(HPF_PORT_KEY, HPF_PORT_DEFAULT).toInt()
        man_port = properties.getProperty(MAN_PORT_KEY, MAN_PORT_DEFAULT).toInt()
        tree_port = properties.getProperty(TREE_PORT_KEY, TREE_PORT_DEFAULT).toInt()
        client_port = properties.getProperty(CLIENT_PORT_KEY, CLIENT_PORT_DEFAULT).toInt()*/
    }
}