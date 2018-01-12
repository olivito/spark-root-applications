# Spark ROOT Applications
Collection of various examples/utilities for Spark ROOT.

## Metrics Listener
Simple Metrics Listener that extends SparkListener. All the jobs are collected in a buffer and dumped into json at the application's end.
Directory/Filename of the metrics output are configurable (see below the conf options)

```
source for MetricsListener is located here: src/main/scala/org/dianahep/sparkrootapplications/metrics
sources for BenchMarksApp are here: src/main/scala/org/dianahep/sparkrootapplications/benchmarks

./spark-submit  --packages org.diana-hep:spark-root_2.11:0.1.7,org.diana-hep:histogrammar-sparksql_2.11:1.0.3,org.json4s:json4s-native_2.11:3.2.11  --class "org.dianahep.sparkrootapplications.benchmarks.AODPublicBenchmarkApp"  --conf spark.extraListeners="org.dianahep.sparkrootapplications.metrics.MetricsListener" --conf spark.executorEnv.pathToMetrics=/Users/vk/software/diana-hep/intel/metrics --conf spark.executorEnv.metricsFileName=test.json /Users/vk/software/diana-hep/spark-root-applications/target/scala-2.11/spark-root-applications_2.11-0.0.1.jar  file:/Users/vk/software/diana-hep/test_data/0029E804-C77C-E011-BA94-00215E22239A.root  /Users/vk/software/diana-hep/test_data/benchmarks/test 1 5
```

## Example Applications
- ReductionExample App
- AODPublicExample App
- Bacon/Higgs Example Apps

## Compiling Applications

Make sure sbt is installed.  See [the documentation here](http://www.scala-sbt.org/1.0/docs/Setup.html).

Check out this repository:
```
git clone git@github.com:olivito/spark-root-applications.git
cd spark-root-applications 
```

Build the jar file containing all the applications:
```
sbt package
```

## Running Applications

Here's an example command to run the DimuonReductionAOD application in local mode (e.g. on a laptop).

```
spark-submit \
--class org.dianahep.sparkrootapplications.examples.DimuonReductionAOD \
--master local[1] \
--packages org.diana-hep:spark-root_2.11:0.1.15 \
target/scala-2.11/spark-root-applications_2.11-0.0.8.jar \
file:/home/olivito/datasci/spark/data/dy_AOD.root \
file:/tmp/ \
DYTest
```

The example input file comes from the CMS 2012 open dataset.  To copy it on lxplus from eos (note that it's a 4 GB file):
```
eoscp root://eospublic.cern.ch//eos/opendata/cms/MonteCarlo2012/Summer12_DR53X/DYJetsToLL_M-50_TuneZ2Star_8TeV-madgraph-tarball/AODSIM/PU_RD1_START53_V7N-v1/20000/DCF94DC3-42CE-E211-867A-001E67398011.root /tmp/${USER}/dy_AOD.root
```

For more information on spark-submit, see [the documentation here](https://spark.apache.org/docs/latest/submitting-applications.html).

### Running on CERN Analytix Cluster

Setup:
```
ssh lxplus.cern.ch
ssh analytix.cern.ch
source /cvmfs/sft.cern.ch/lcg/views/LCG_88/x86_64-slc6-gcc49-opt/setup.sh
source /cvmfs/sft.cern.ch/lcg/etc/hadoop-confext/hadoop-setconf.sh analytix
export KRB5CCNAME=FILE:/tmp/${USER}_krb
kinit -c /tmp/${USER}_krb
```

Running Viktor's example application:
```
spark-submit --master yarn \
--class "org.dianahep.sparkrootapplications.examples.ReductionExampleApp" \
--files $KRB5CCNAME#krbcache \
--conf spark.executorEnv.KRB5CCNAME='FILE:$PWD/krbcache' \
--conf spark.driver.extraClassPath="/usr/lib/hadoop/EOSfs.jar" \
--packages org.diana-hep:spark-root_2.11:0.1.11,org.diana-hep:histogrammar-sparksql_2.11:1.0.3 \
/afs/cern.ch/user/o/olivito/public/spark/spark-root-applications_2.11-0.0.3.jar \
root://eospublic.cern.ch://eos/opendata/cms/Run2010B/MuOnia/AOD/Apr21ReReco-v1/0000/FEF1B99B-BF77-E011-B0A0-E41F1318174C.root
```

Syntax for running DimuonReductionAOD:
```
spark-submit [options]
/afs/cern.ch/user/o/olivito/public/spark/spark-root-applications_2.11-0.0.8.jar \
<input_directory> \
<output_directory> \
<output_label>
```

Example of running the DimuonReductionAOD application.  Note that this only works properly running over a full directory, not on a single file, because of some eos/xrootd issue at the moment..
```
spark-submit --master yarn \
--class org.dianahep.sparkrootapplications.examples.DimuonReductionAOD \
--packages org.diana-hep:spark-root_2.11:0.1.15 \
--conf spark.driver.extraClassPath="/usr/lib/hadoop/EOSfs.jar" \
--files $KRB5CCNAME#krbcache \
--conf spark.executorEnv.KRB5CCNAME='FILE:$PWD/krbcache' \
/afs/cern.ch/user/o/olivito/public/spark/spark-root-applications_2.11-0.0.8.jar \
root://eospublic.cern.ch//eos/opendata/cms/MonteCarlo2012/Summer12_DR53X/ZZTo4mu_8TeV-powheg-pythia6/AODSIM/PU_RD1_START53_V7N-v1/20000/ \
hdfs:/cms/bigdatasci/olivito/sparktest/ \
ZZTo4mu
```

Another example, running over all the Run2012B SingleMu data. Note the wildcard usage to grab only directories with root files (directories and not files, because of the bug mentioned above). 
```
spark-submit --master yarn \
--class org.dianahep.sparkrootapplications.examples.DimuonReductionAOD \
--packages org.diana-hep:spark-root_2.11:0.1.15 \
--conf spark.driver.extraClassPath="/usr/lib/hadoop/EOSfs.jar" \
--files $KRB5CCNAME#krbcache \
--conf spark.executorEnv.KRB5CCNAME='FILE:$PWD/krbcache' \
/afs/cern.ch/user/o/olivito/public/spark/spark-root-applications_2.11-0.0.8.jar \
root://eospublic.cern.ch//eos/opendata/cms/Run2012B/SingleMu/AOD/22Jan2013-v1/*0*/ \
hdfs:/cms/bigdatasci/olivito/sparktest/ \
SingleMu_Run2012B
```

### Running on multiple samples in one spark job

Small scale example for running over MC multiple samples in one spark job. The samples are specified in the input csv file, here a subset of ZZTo4mu and ZZTo2e2mu. 
```
spark-submit --master yarn \
--class org.dianahep.sparkrootapplications.examples.DimuonReductionAODMultiDataset \
--packages org.diana-hep:spark-root_2.11:0.1.15 \
--conf spark.driver.extraClassPath="/usr/lib/hadoop/EOSfs.jar" \
--files $KRB5CCNAME#krbcache \
--conf spark.executorEnv.KRB5CCNAME='FILE:$PWD/krbcache' \
/afs/cern.ch/user/o/olivito/public/spark/spark-root-applications_2.11-0.0.9.jar \
/afs/cern.ch/user/o/olivito/public/spark/input_datasets_cluster_test.csv \
hdfs:/cms/bigdatasci/olivito/sparktest/ 
```

Because the data and MC AODs have different numbers of branches in the root trees, the corresponding Spark Datasets cannot simply be merged in a single job.  For now, I'm handling this by just defining two separate spark jobs, one for data and one for MC.

To run over data, use this input csv file:
```
/afs/cern.ch/user/o/olivito/public/spark/input_datasets_data.csv
```

To run over MC, use this file:
```
/afs/cern.ch/user/o/olivito/public/spark/input_datasets_mc.csv

```

## Merging output parquet files

Using the library `parquet-tools`. Note that prefix `hdfs:` isn't necessary here.
```
hadoop jar \
/afs/cern.ch/user/o/olivito/public/spark/parquet-tools-1.9.0.jar \
merge \
/cms/bigdatasci/olivito/sparktest/dimuonReduced_ZZTo4mu_171220_134613/mll.parquet/ \
/cms/bigdatasci/olivito/sparktest/dimuonReduced_ZZTo4mu_171220_134613/mll_merged.parquet/merged.parquet
```

Also note that this can only handle a limited number of files.  500 is ok, 4500 is not.