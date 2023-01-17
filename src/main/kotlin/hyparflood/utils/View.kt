package hyparflood.utils

import org.apache.logging.log4j.LogManager
import pt.unl.fct.di.novasys.network.data.Host
import java.util.*


class View(
    private val name: String,
    private val capacity: Int,
    private val self: Host,
    private val rnd: Random,
) : IView {

    companion object {
        private val logger = LogManager.getLogger()
    }

    private val peers: MutableSet<Host> = mutableSetOf()

    private lateinit var other: IView
    private lateinit var pending: Set<Host>

    override fun setOther(other: IView, pending: Set<Host>) {
        this.other = other
        this.pending = pending
    }

    override fun toString(): String {
        return peers.toString()
    }

    override fun addPeer(peer: Host): Host? {
        if (peer != self && !peers.contains(peer) && !other.containsPeer(peer) && !pending.contains(peer)) {
            var excess: Host? = null
            if (peers.size == capacity) excess = dropRandom()
            peers.add(peer)
            logger.info("{} Added {}", name.uppercase(), peer.address.hostAddress)
            return excess
        }
        return null
    }

    override fun removePeer(peer: Host): Boolean {
        val removed = peers.remove(peer)
        if (removed)
            logger.info("{} Removed {}", name.uppercase(), peer.address.hostAddress)
        return removed
    }

    override fun containsPeer(peer: Host): Boolean {
        return peers.contains(peer)
    }

    override fun dropRandom(): Host? {
        var torm: Host? = null
        if (peers.size > 0) {
            val idx = rnd.nextInt(peers.size)
            val hosts = peers.toTypedArray()
            torm = hosts[idx]
            peers.remove(torm)
            logger.info("{} Removed {}", name.uppercase(), torm.address.hostAddress)
        }
        return torm
    }

    override fun getPeers(): Set<Host> {
        return HashSet(peers)
    }

    override fun getRandomSample(sampleSize: Int): Set<Host> {
        val ret: Set<Host> = if (peers.size > sampleSize) {
            val hosts: MutableList<Host> = ArrayList(peers)
            while (hosts.size > sampleSize) hosts.removeAt(rnd.nextInt(hosts.size))
            HashSet(hosts)
        } else peers
        return HashSet(ret)
    }

    override fun getRandom(): Host? {
        return if (peers.size > 0) {
            val idx = rnd.nextInt(peers.size)
            val hosts = peers.toTypedArray()
            hosts[idx]
        } else null
    }

    override fun getRandomDiff(from: Host): Host? {
        val hosts: MutableList<Host> = ArrayList(peers)
        hosts.remove(from)
        return if (hosts.size > 0) hosts[rnd.nextInt(hosts.size)] else null
    }

    override fun fullWithPending(pending: Set<Host>): Boolean {
        return peers.size + pending.size >= capacity
    }

    override fun isFull(): Boolean {
        return peers.size >= capacity
    }

    override fun isEmpty(): Boolean {
        return peers.isEmpty()
    }
}