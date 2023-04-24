package tree.utils

import pt.unl.fct.di.novasys.network.data.Host

abstract class ChildState(val child: Host)

class ChildSync(child: Host) : ChildState(child)

class ChildReady(child: Host, var childStableTime: HybridTimestamp = HybridTimestamp()) : ChildState(child)



