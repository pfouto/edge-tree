package manager

import kotlinx.serialization.Serializable

@Serializable
data class StaticTree(val tree: Map<String, List<String>>)