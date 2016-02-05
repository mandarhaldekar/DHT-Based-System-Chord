#Distributed peer-to-peer system implementing <a href="https://en.wikipedia.org/wiki/Chord_(peer-to-peer)">CHORD</a> protocol using Go and JSON-RPC

Nodes in this distributed system holds data in form of triplets i.e. key, relation, value. Triplets are assigned to nodes in CHORD rings based on hash value on key and relation.

This system supports following operations: lookup, insert, insertOrUpdate, delete, partial lookup etc. This is dymanic system where nodes can leave and join in scheduled manner without any loss of data
