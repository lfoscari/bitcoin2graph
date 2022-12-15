1. Customize Parameters to set the important fields and the directories in use,
   then navigate to the resources' directory.

2. Download all inputs and outputs needed with
    $ wget -nv --show-progress --directory-prefix inputs --input-file input-urls.txt
    $ wget -nv --show-progress --directory-prefix outputs --input-file output-urls.txt

3. Change their filename with
    $ find inputs -type f -exec sh -c 'mv $1 ${1%\?*}' sh {} \;
    $ find outputs -type f -exec sh -c 'mv $1 ${1%\?*}' sh {} \;

3. Decompress the downloaded files with
    $ gunzip -r -S ".gz" inputs
    $ gunzip -r -S ".gz" outputs

4. Run ParseTSVs to remove unnecessary data, compute the bloom filters
   and split the data into more compact chunks.

5. Run AddressMap to build the address-to-long map, used by webgraph to
   build the transaction graph, skip this step otherwise.

5. Run either FindMapping to view the mappings in stdin or
   Blockchain2Webgraph to save the results in the Webgraph format.

TODO: keep track of the inputs and outputs already added to the graph.
