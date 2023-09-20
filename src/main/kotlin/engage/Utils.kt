package engage


interface ImmutableInteger {
    fun getValue(): Int
}


class MutableInteger(var value: Int = 0) : ImmutableInteger {

    override fun getValue(): Int {
        return value
    }

    fun setValue(value: Int) {
        this.value = value
    }

    override fun toString(): String {
        return "MI{$value}"
    }

}