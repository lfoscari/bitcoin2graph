How to use:

1. Open DownloadInputsOutputs.java and set the number of inputs and outputs you want to download
2. Be sure to have in the input-urls.txt and output-urls.txt enough lines
3. Run DownloadInputsOutputs.java, the results will be in the 'originals' directory
4. TSVClean will organize the data into "inputs" and "outputs" and remove the unnecessary data
5. Now generate the bloom filters to improve performance by running TransactionBloom
6. The last step is to find the mappings with FindMapping