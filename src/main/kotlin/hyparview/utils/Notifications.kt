package hyparview.utils

import pt.unl.fct.di.novasys.babel.generic.ProtoNotification
import pt.unl.fct.di.novasys.network.data.Host


class NeighbourDown(val neighbour: Host) : ProtoNotification(NOTIFICATION_ID) {

    companion object {
        const val NOTIFICATION_ID: Short = 301
    }
}

class NeighbourUp(val neighbour: Host) : ProtoNotification(NOTIFICATION_ID) {

    companion object {
        const val NOTIFICATION_ID: Short = 302
    }
}