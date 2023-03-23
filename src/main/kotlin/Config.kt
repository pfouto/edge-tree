import java.util.*

class Config(properties: Properties) {

    val hpf_arwl: Int
    val hpf_prwl: Int
    val hpf_shuffle_time: Int
    val hpf_hello_backoff: Int
    val hpf_join_timeout: Int
    val hpf_k_active: Int
    val hpf_k_passive: Int
    val hpf_active_view: Int
    val hpf_passive_view: Int

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


    }


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


    }

}