How to use:

1. Open DownloadInputsOutputs.java and set the number of inputs and outputs you want to download
2. Be sure to have enough lines in input-urls.txt and output-urls.txt
3. Run DownloadInputsOutputs.java, the results will be in the "originals" directory
4. Run TSVClean to organize the data into "inputs" and "outputs" and remove the unnecessary data
5. Run TransactionBloom to generate the bloom filters to improve performance
6. You can either run FindMapping directly to view the results in the command line or run Blockchain2Webgraph to convert
the portion of the blockchain you downloaded into webgraph format, if you choose this option remember to run
AddressesFromOriginals and AddressToLong in this order to extract unique addresses from your files and generate a map
from addresses to longs, which is needed by Webgraph.

TODO: streamline this process