# Notes

Bi-directed graph database in sorted key-store.

* Each entry follows `long nodeId, byte edgeLabel, long otherId`.  
* For every entry, an equivalent `long otherId, byte edgeLabel ^ 0x80, long nodeId` is inserted.

The theory is that queries can be traversed efficiently from multiple terminal nodes at once by placing iterators on
each node and intersecting their outputs with an n-ary search.

```
A - o

iter(A,-)
```

```
A - o + o

iter(iter(A,-),+)
```

```
A - o + B

zip(iter(A,-),iter(B,+))
```

```
A - o + o ~ B

zip(iter(iter(A,-),+),iter(B,~))
```

```
    C
    |
A - o + B

zip(iter(C,|),iter(A,-),iter(B,+))
```
```
Fixed collisions
  A  B  C
1 x
2    x
3 x     x
4 x  x  x
5    x  x

A>1
A>3
A>4

B>2
B>4
B>5

C>3
C>4
C>5

1<A

2<B

3<A
3<C

4<A
4<B
4<C

5<B
5<C
```

```
Sparse streamed collisions
    |   |   |   |
    A B C D E F G
  1 x   x x
- 2   x O
  3 x       x
  4   x   x     x
  5   x       x
- 6     O x O
  7 x         x x
- 8   x O   O
```

1. Always go to next lowest-power out of date node!




Using zig-zag varints for the pointer? Idea: if both sides of the 




public Concept createSomething(Transaction tx)

Attribute.create()
