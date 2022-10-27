package manager

import pt.unl.fct.di.novasys.babel.generic.ProtoRequest

class ChildRequest() : ProtoRequest(ID) {
    companion object {
        const val ID: Short = 101
    }
}