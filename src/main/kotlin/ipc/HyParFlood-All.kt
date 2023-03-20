package ipc

import pt.unl.fct.di.novasys.babel.generic.ProtoNotification
import pt.unl.fct.di.novasys.network.data.Host

class NeighbourDown(val neighbour: Host) : ProtoNotification(ID) {

    companion object {
        const val ID: Short = 301
    }
}

class NeighbourUp(val neighbour: Host) : ProtoNotification(ID) {

    companion object {
        const val ID: Short = 302
    }
}