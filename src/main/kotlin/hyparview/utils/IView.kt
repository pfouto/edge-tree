package hyparview.utils

import pt.unl.fct.di.novasys.network.data.Host

interface IView {
    fun setOther(other: IView, pending: Set<Host>)

    fun addPeer(peer: Host): Host?

    fun removePeer(peer: Host): Boolean

    fun containsPeer(peer: Host): Boolean

    fun dropRandom(): Host?

    fun getRandomSample(sampleSize: Int): Set<Host>

    fun getPeers(): Set<Host>

    fun getRandom(): Host?

    fun getRandomDiff(from: Host): Host?

    fun fullWithPending(pending: Set<Host>): Boolean

    fun isFull(): Boolean

    fun isEmpty(): Boolean
}