1. Customize Parameters to set the important fields and the directories in use

2. Download all inputs and outputs needed with
    $ wget -nv --show-progress --directory-prefix inputs --input-file input-urls.txt
    $ wget -nv --show-progress --directory-prefix outputs --input-file output-urls.txt

3. Untar the downloaded files with
    $ find inputs -type f -exec gunzip {} ${{}:0:48} \;
    $ find outputs -type f -delete -exec gunzip {} \;

4. Clean only keep the right information with
    $ cat inputs/ | awk -F'\t' '{if ($10 == "0") { print $13, $7 }}' | split -d -l {{ MAX_TSV_LINES }}
    $ cat outputs/ | awk -F'\t' '{if ($10 == "0") { print $2, $7 }}' | split -d -l {{ MAX_TSV_LINES }}

5. Run BloomFilters to compute the bloom filters

3. Run either FindMapping to view the mappings in stdin or
   Blockchain2Webgraph to save the results in the Webgraph format.

TODO: keep track of the inputs and outputs already added to the graph.
