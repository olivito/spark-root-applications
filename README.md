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
target/scala-2.11/spark-root-applications_2.11-0.0.3.jar \
file:/home/olivito/datasci/spark/data/dy_AOD.root
```

The example input file comes from the CMS 2012 open dataset.  To copy it on lxplus from eos (note that it's a 4 GB file):
```
eoscp root://eospublic.cern.ch//eos/opendata/cms/MonteCarlo2012/Summer12_DR53X/DYJetsToLL_M-50_TuneZ2Star_8TeV-madgraph-tarball/AODSIM/PU_RD1_START53_V7N-v1/20000/DCF94DC3-42CE-E211-867A-001E67398011.root /tmp/${USER}/dy_AOD.root
```

For more information on spark-submit, see [the documentation here](https://spark.apache.org/docs/latest/submitting-applications.html).