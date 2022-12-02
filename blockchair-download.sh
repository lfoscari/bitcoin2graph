resources="./src/main/resources"
max_tsv_lines=10
tab="\011"

# Inputs

wget -nv --show-progress --directory-prefix $resources/inputs --input-file input-urls-small.txt

find $resources/inputs -type f -exec sh -c 'mv $1 ${1%\?*}' sh {} \;

gunzip -S ".gz" -r $resources/inputs

mkdir $resources/inputs/chunks

find $resources/inputs -type f -name "*.tsv" | xargs -n 1 tail -n +2 | awk -F'\t' '{if ($10 == "0") { print $13 $tab $7 }}' | \
  split -d -l $max_tsv_lines --additional-suffix=.tsv - $resources/inputs/chunks/

# Outputs

wget -nv --show-progress --directory-prefix $resources/outputs --input-file output-urls-small.txt

find $resources/outputs -type f -exec sh -c 'mv $1 ${1%\?*}' sh {} \;

gunzip -S ".gz" -r $resources/outputs

mkdir $resources/outputs/chunks

find $resources/outputs -type f -name "*.tsv" | xargs -n 1 tail -n +2 | awk -F'\t' '{if ($10 == "0") { print $13 $tab $7 }}' | \
  split -d -l $max_tsv_lines --additional-suffix=.tsv - $resources/outputs/chunks/