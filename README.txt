The best solution would be to implement a extension of ImmutableGraph, the
skeleton is in BlockchainGraph.

ScatteredArcsASCIIGraph on the other hand simply needs a list of arcs, to make
it lazy I implemented it with a custom iterator over the blockchain.  The main
drawback is that two passes are necessary over the whole blockchain, because
the blocks are parsed from last to first, instead of from first to last, making
it impossible to close the mappings, because a mapping references a previous.
AFAIK Bitcoinj does not allow reverse loading, but another solution might be
found.

In case the association TransactionOutLog - Address uses too much space,
consider using an implementation of RocksDB or LevelDB.

TODO:
- Figure out how to add information to arcs and nodes
- Avoid scanning two times the blocks for performance
- Find a better conversion from address to integer with graceful degradation