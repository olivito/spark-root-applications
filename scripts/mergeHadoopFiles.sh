#!/bin/bash

INPUT_DIR=$1
OUTPUT_DIR=$2
NFILES_PER_MERGE=${3:-500} # default to 500 files per merge chunk

# make input file list
TMPFILE="tmp_filelist.txt"
rm -f ${TMPFILE}*
hadoop fs -ls ${INPUT_DIR} | awk '{print $8}' | grep ".snappy.parquet" > ${TMPFILE}
# add prefix
sed -i -e 's/^/hdfs:/' ${TMPFILE}

# split into file lists with N lines each, configured above
split -l ${NFILES_PER_MERGE} -d ${TMPFILE} ${TMPFILE}_part
rm -f ${TMPFILE}

FILE_LISTS=`/bin/ls ./${TMPFILE}_part*`
OUTPUT_FILE_LIST=()

COMMAND_BASE="hadoop jar /afs/cern.ch/user/o/olivito/public/spark/parquet-tools-1.9.0.jar merge "

# first step of merging to reduce number of input files
for f in ${FILE_LISTS}
do
    PART=${f:(-6)}
    # input files
    COMMAND="${COMMAND_BASE} `cat ${f}`"
    # output file
    OUTPUT_FILE="hdfs:${OUTPUT_DIR}_${PART}/merged.parquet"
    OUTPUT_FILE_LIST+=(${OUTPUT_FILE})
    COMMAND+=" ${OUTPUT_FILE}"
    #echo ${COMMAND}
    echo "merging ${PART}"
    ${COMMAND}
    rm -f ${f}
done

# now merge files again to get a single file
# input files
COMMAND="${COMMAND_BASE} ${OUTPUT_FILE_LIST[@]}"
# output directory
COMMAND+=" hdfs:${OUTPUT_DIR}/merged.parquet"
#echo ${COMMAND}
echo "doing final merge"
${COMMAND}

# provide commands to delete the input directories
echo ""
echo "Input files merged from"
echo "${INPUT_DIR}"
echo "to"
echo "${OUTPUT_DIR}"
echo ""
echo "If you want to delete the input unmerged files:"
echo "hadoop fs -rm -r ${INPUT_DIR}"
echo ""
echo "If you want to delete the intermediate merged files:"
echo "hadoop fs -rm -r ${OUTPUT_DIR}_part*"
echo ""
