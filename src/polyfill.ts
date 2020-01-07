interface Array<T> {
    flatMap<E>(callback: (t: T) => Array<E>): Array<E>
}

if (!Array.prototype.flatMap) {
    Object.defineProperty(Array.prototype, 'flatMap', {
        value: function(f: Function) {
            return this.reduce((ys: any, x: any) => {
                return ys.concat(f.call(this, x))
            }, [])
        },
        enumerable: false,
    })
}