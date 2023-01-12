package hyparview.utils

import pt.unl.fct.di.novasys.babel.generic.ProtoRequest
import java.net.Inet4Address

class InitRequest(val address: Inet4Address) : ProtoRequest(ID) {
    companion object {
        const val ID: Short = 301
    }
}