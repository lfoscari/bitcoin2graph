TODO:
- Avoid skipping duplicate arcs for performance reasons
- Figure out how to add informations to arcs and nodes
- Avoid scanning two times the blocks for performance
- Find a better conversion from address to integer with graceful degradation
- Figure out how to work with webgraph-big
- Find a more efficient way to calculate the total number of nodes

Implement an Immutable graph and override the nodeIterator class to create a
webgraph compatible representation. Otherwise check out ScatteredArcsASCIIGraph
to simply supply a list of arcs and use a mapping function from Address to
Long.

In case the association TransactionOutLog - Address uses too much space,
consider using an implementation of RocksDB or LevelDB.