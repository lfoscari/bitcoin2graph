Check out Parameters.java to customize the execution.

The parser works with three passes over all the blockchain blocks, each one does not need to be repeated once completed,
unless the blockchain changes. The first pass builds an "address conversion" database to map addresses to longs, this
is crucial because Webgraph accepts arcs in the form of longs (note: in the final graph the nodes won't be labeled
exactly as this map suggests, because during construction new numbers are associated to the nodes to improve compression).

The second pass populates the address mappings, because of how the Bitcoin blockchain is structured the transactions
don't contain address-to-address associations but store as the sender a transaction id and an index, meaning that the
sender is actually the i-th address in the given transaction.

The last pass completes the mappings by running once again over all the blocks at completing the association between
addresses and exposes an iterator, which is given to the ScatteredArcsASCIIGraph constructor.

The main issue encountered with the last to passes was to execute them concurrently, but reading the block files one at
a time (the code was to be run on an HDD machine) and avoid filling up too much memory, automatically resorting to swap
space, even if the latter challenge is yet to be conquered, the execution is quite fast and easily tunable.

Because of how the block loader works it keeps in memory at most two times the number of threads in block files, each
sized at roughly 128MB, keep that in mind when sizing the number of threads.

As a database we opted for RocksDB, due to its graceful degradation features.