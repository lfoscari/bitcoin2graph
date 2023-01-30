Download inputs and outputs from Blockchair.

Scan the outputs to create a transaction->addresses association.
Scan the inputs to find the tuples in the form (transaction, address), then find the same the transaction in the outputs
association and associate the related address to the current address.

To find the total number of nodes you can count the lines in addresses.tsv from Blockchair.
At the moment the problem is that the node identifiers are both positive and negative, also there is no
effort to keep compress the nodes. Both problem can be mitigated with a node to int map.