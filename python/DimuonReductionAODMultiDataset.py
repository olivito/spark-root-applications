"""DimuonReductionAODMultiDataset.py"""
from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.functions import lit
from pyspark.sql import Row
from math import *
import sys, os, csv, time

# helper function to calculate isolation from the struct
def iso(isoStruct): 
    neutral = max(0.0, isoStruct.sumNeutralHadronEt + isoStruct.sumPhotonEt - 0.5 * isoStruct.sumPUPt)
    return isoStruct.sumChargedHadronPt + neutral

# just do the looser muon selection here.  will do a second selection for leading muon later
def passMuonSel(muon):
    return ((muon.pt_ > 10.0) and 
        (fabs(muon.eta_) < 2.4) and
        (iso(muon.pfIsolationR04_)/muon.pt_ < 0.5) and
        ((muon.type_ & (1<<1)) != 0)) # global muon 

# method to apply the event level selection cuts,
#  including tighter cuts on the leading muon
def passEventSel(muons):
    return ((len(muons) > 1) and
            (muons[0].pt_ > 25.0) and
            (fabs(muons[0].eta_) < 2.1) and
            (iso(muons[0].pfIsolationR04_)/muons[0].pt_ < 0.12) and
            (muons[0].pdgId_ * muons[1].pdgId_ < 0))

# simplified formula, assuming E >> m
def invariantMass(mu1, mu2):
    return sqrt(2*mu1.pt_*mu2.pt_*(cosh(mu1.eta_-mu2.eta_)-cos(mu1.phi_-mu2.phi_)))
    
def handleEvent(event):
    # first select muons
    selMuons = [muon for muon in event.muons if passMuonSel(muon)]
    # sort in decreasing order of muon pT - makes a noticeable difference in how many events pass
    # if not sorting, can reproduce the scala results exactly
    sortedMuons = sorted(selMuons, key=lambda muon: -muon.pt_)
    #sortedMuons = selMuons
    # check if event passes selection (including requiring at least 2 muons)
    if passEventSel(sortedMuons):
        return [Row(mll=invariantMass(sortedMuons[0], sortedMuons[1]),sampleID=event.sampleID)]
    else:
        return []

# primary method to do the dataset reduction
def doReduction(configFile,outputPath):

    sc = SparkContext("yarn", "DimuonReductionAODMultiDataset Large Scale App")
    sqlContext = SQLContext(sc)

    samples_list = []
    with open(configFile) as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            samples_list.append(row)

    dsAll = None

    for sample in samples_list:
        dsTemp = sqlContext.read.format("org.dianahep.sparkroot.experimental").option("tree","Events").load(sample['path']).withColumn("sampleID",lit(sample['sampleID']))
        dsMuons = dsTemp.select("recoMuons_muons__RECO_.recoMuons_muons__RECO_obj","sampleID").toDF("muons","sampleID")
        sample['count'] = dsMuons.count()
        if not dsAll: dsAll = dsMuons
        else: dsAll = dsAll.union(dsMuons)

    # get current time for output file naming
    timestring = time.strftime("%y%m%d_%H%M%S", time.gmtime())
    outputDir = '%s/reduction_test_%s'%(outputPath,timestring)

    # write out samples with counts as a parquet file (could also do csv..)
    dfCounts = sc.parallelize(samples_list).toDF()
    dfCounts.write.parquet('%s/count.parquet'%(outputDir), mode="overwrite")

    dsMll = sqlContext.createDataFrame(dsAll.rdd.flatMap(handleEvent))
    dsMll.write.parquet('%s/mll.parquet'%(outputDir), mode="overwrite")
    

if __name__ == "__main__":
    if len(sys.argv) < 3: 
        print 'usage: spark-submit DimuonReductionAODMultiDataset.py <configFile> <outputPath>'
        exit()
    configFile = sys.argv[1]
    outputPath = sys.argv[2]

    doReduction(configFile,outputPath)
