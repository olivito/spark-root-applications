package org.dianahep.sparkrootapplications.examples

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.lit

import scala.math._
import scala.io.Source

import org.dianahep.sparkroot.experimental._

import java.util.Date
import java.text.SimpleDateFormat
import java.util.Calendar

object DimuonReductionAODMultiDataset {

  // ------------------------------------------------------------------------------------------
  // case classes for muon branches

  case class Record2 (
    fX : Float,
    fY : Float,
    fZ : Float
  )
  case class Record1 (
    fCoordinates : Record2
  )
  case class Record4 (
    processIndex_ : Short,
    productIndex_ : Short,
    elementIndex_ : Int
  )
  case class Record3 (
    recoMuons_muons__RECO_obj_innerTrack__product_ : Record4
  )
  case class Record6 (
    processIndex_ : Short,
    productIndex_ : Short,
    elementIndex_ : Int
  )
  case class Record5 (
    recoMuons_muons__RECO_obj_outerTrack__product_ : Record6
  )
  case class Record8 (
    processIndex_ : Short,
    productIndex_ : Short,
    elementIndex_ : Int
  )
  case class Record7 (
    recoMuons_muons__RECO_obj_globalTrack__product_ : Record8
  )
  case class Record10 (
    processIndex_ : Short,
    productIndex_ : Short,
    elementIndex_ : Int
  )
  case class Record9 (
    product_ : Record10
  )
  case class Record13 (
    fX : Float,
    fY : Float,
    fZ : Float
  )
  case class Record12 (
    fCoordinates : Record13
  )
  case class Record15 (
    fX : Float,
    fY : Float,
    fZ : Float
  )
  case class Record14 (
    fCoordinates : Record15
  )
  case class Record16 (
    id_ : Int
  )
  case class Record17 (
    id_ : Int
  )
  case class Record11 (
    tower : Float,
    towerS9 : Float,
    em : Float,
    emS9 : Float,
    emS25 : Float,
    emMax : Float,
    had : Float,
    hadS9 : Float,
    hadMax : Float,
    ho : Float,
    hoS9 : Float,
    ecal_time : Float,
    ecal_timeError : Float,
    hcal_time : Float,
    hcal_timeError : Float,
    ecal_position : Record12,
    hcal_position : Record14,
    ecal_id : Record16,
    hcal_id : Record17
  )
  case class Record20 (
    fX : Double,
    fY : Double,
    fZ : Double
  )
  case class Record19 (
    fCoordinates : Record20
  )
  case class Record22 (
    fX : Double,
    fY : Double,
    fZ : Double
  )
  case class Record21 (
    fCoordinates : Record22
  )
  case class Record18 (
    updatedSta : Boolean,
    trkKink : Float,
    glbKink : Float,
    trkRelChi2 : Float,
    staRelChi2 : Float,
    chi2LocalPosition : Float,
    chi2LocalMomentum : Float,
    localDistance : Float,
    globalDeltaEtaPhi : Float,
    tightMatch : Boolean,
    glbTrackProbability : Float,
    tkKink_position : Record19,
    glbKink_position : Record21
  )
  case class Record23 (
    nDof : Int,
    timeAtIpInOut : Float,
    timeAtIpInOutErr : Float,
    timeAtIpOutIn : Float,
    timeAtIpOutInErr : Float
  )
  case class Record24 (
    sumPt : Float,
    emEt : Float,
    hadEt : Float,
    hoEt : Float,
    nTracks : Int,
    nJets : Int,
    trackerVetoPt : Float,
    emVetoEt : Float,
    hadVetoEt : Float,
    hoVetoEt : Float
  )
  case class Record25 (
    sumPt : Float,
    emEt : Float,
    hadEt : Float,
    hoEt : Float,
    nTracks : Int,
    nJets : Int,
    trackerVetoPt : Float,
    emVetoEt : Float,
    hadVetoEt : Float,
    hoVetoEt : Float
  )
  case class Record26 (
    sumChargedHadronPt : Float,
    sumChargedParticlePt : Float,
    sumNeutralHadronEt : Float,
    sumPhotonEt : Float,
    sumNeutralHadronEtHighThreshold : Float,
    sumPhotonEtHighThreshold : Float,
    sumPUPt : Float
  )
  case class Record27 (
    sumChargedHadronPt : Float,
    sumChargedParticlePt : Float,
    sumNeutralHadronEt : Float,
    sumPhotonEt : Float,
    sumNeutralHadronEtHighThreshold : Float,
    sumPhotonEtHighThreshold : Float,
    sumPUPt : Float
  )
  case class Record29 (
    fX : Double,
    fY : Double,
    fZ : Double,
    fT : Double
  )
  case class Record28 (
    fCoordinates : Record29
  )
  case class Record0 (
    qx3_ : Int,
    pt_ : Float,
    eta_ : Float,
    phi_ : Float,
    mass_ : Float,
    vertex_ : Record1,
    pdgId_ : Int,
    status_ : Int,
    innerTrack_ : Record3,
    outerTrack_ : Record5,
    globalTrack_ : Record7,
    recoMuons_muons__RECO_obj_refittedTrackMap_ : scala.collection.Map[Int, Record9],
    bestTrackType_ : Int,
    calEnergy_ : Record11,
    combinedQuality_ : Record18,
    time_ : Record23,
    energyValid_ : Boolean,
    matchesValid_ : Boolean,
    isolationValid_ : Boolean,
    pfIsolationValid_ : Boolean,
    qualityValid_ : Boolean,
    caloCompatibility_ : Float,
    isolationR03_ : Record24,
    isolationR05_ : Record25,
    pfIsolationR03_ : Record26,
    pfIsolationR04_ : Record27,
    type_ : Int,
    pfP4_ : Record28
  )
  case class Event (
    muons : Seq[Record0],
    sampleID : Int
  )

  case class Output (
    mll : Float,
    sampleID : Int
  )

  // case class for sample configuration
  case class Sample (
    sampleID : Int,
    name : String,
    xsec : Double,
    path : String
  )

  // case class for writing out sample info
  case class SampleCount (
    sampleID : Int,
    name : String,
    xsec : Double,
    count : Long
  )

  // end of case classes
  // ------------------------------------------------------------------------------------------

  // ------------------------------------------------------
  // functions for selection or computation

  // pileup-corrected absolute isolation value
  def iso(isoStruct: Record27):Float = {
    val neutral = max(0.0F, isoStruct.sumNeutralHadronEt + isoStruct.sumPhotonEt - 0.5F * isoStruct.sumPUPt)
    isoStruct.sumChargedHadronPt + neutral
  }

  // just do the looser muon selection here.  will do a second selection for leading muon later
  def passMuonSel(muon: Record0):Boolean = {
    (muon.pt_ > 10.0F) &&
    (abs(muon.eta_) < 2.4F) &&
    (iso(muon.pfIsolationR04_)/muon.pt_ < 0.5F) &&
    ((muon.type_ & (1<<1)) != 0) // global muon
  }

  // method to apply the event level selection cuts,
  //  including tighter cuts on the leading muon
  def passEventSel(event:Event):Boolean = {
    (event.muons.length > 1) &&
    (event.muons(0).pt_ > 25.0F) &&
    (abs(event.muons(0).eta_) < 2.1F) &&
    (iso(event.muons(0).pfIsolationR04_)/event.muons(0).pt_ < 0.12F) &&
    (event.muons(0).pdgId_ * event.muons(1).pdgId_ < 0)
  }

  def invariantMass(mu1:Record0,mu2:Record0):Float = {
    val pt1 = mu1.pt_
    val phi1 = mu1.phi_
    val eta1 = mu1.eta_
    val pt2 = mu2.pt_
    val phi2 = mu2.phi_
    val eta2 = mu2.eta_
    // simplified formula, assuming E >> m
    sqrt(2*pt1*pt2*(cosh(eta1-eta2)-cos(phi1-phi2))).toFloat
  }
  // ------------------------------------------------------

  // main function to be executed
  def main(args: Array[String]) {
    val configFile = args(0)
    val outputPath = args(1)
    val spark = SparkSession.builder()
      .appName("AOD Public DS Example")
      .getOrCreate()
    val sc = spark.sparkContext

    // for case classes inside Datasets
    import spark.implicits._

    // read in configuration csv file to dataset.  Use case class Sample as schema
    val bufferedSource = Source.fromFile(configFile)
    val sampleBuf = scala.collection.mutable.ListBuffer.empty[Sample]
    for (line <- bufferedSource.getLines) {
      val cols = line.split(",").map(_.trim)
      // protection for empty lines
      if (cols.length == 4) {
        sampleBuf += Sample(cols(0).toInt,cols(1).toString,cols(2).toDouble,cols(3).toString)
      }
    }

    // empty mutable buffer to store number of events in each sample
    val neventsBuf = scala.collection.mutable.ListBuffer.empty[Long]

    // loop over samples to create big dataset. Note that dsAll still has class Dataset (not DataFrame) at the end.
    var dsAll: org.apache.spark.sql.Dataset[Event] = null;
    for (sample <- sampleBuf) {
      // add column to keep track of sampleID
      val dsTemp = spark.sqlContext.read.option("tree", "Events").root(sample.path)
        .withColumn("sampleID",lit(sample.sampleID))
      // select the muon branches and convert to Dataset[Event]
      val dsMuons = dsTemp.select("recoMuons_muons__RECO_.recoMuons_muons__RECO_obj","sampleID").toDF("muons","sampleID").as[Event]
      // count nevents in each sample
      neventsBuf += dsMuons.count
      if (dsAll!= null) {
        dsAll = dsAll.union(dsMuons)
      } else {
        dsAll = dsMuons
      }
    } // loop over samples

    // store the number of events before any selection, along with basic sample info, for normalization later
    // store in a Dataframe to easily write to a parquet file.. 
    val samplesCounts = (sampleBuf zip neventsBuf).map{
      case (sample,count) => SampleCount(sample.sampleID,sample.name,sample.xsec,count)
    }
    val dsSamples = sc.parallelize(samplesCounts).toDF()

    // get current date and time
    val now = Calendar.getInstance().getTime();
    val dateFormatter = new SimpleDateFormat("YYMMdd_HHmmss");
    val date = dateFormatter.format(now)

    // write counts to output files
    // is there a way to write a simple Seq to a parquet file instead?
    val parquetFilenameCount = outputPath + "/dimuonReduced_" + date + "/count.parquet"
    dsSamples.write.format("parquet").save(parquetFilenameCount)

    // select passing muon objects
    val dsMuonsSel = dsAll.map{
      event =>
      val pass_muons = event.muons.filter(muon => passMuonSel(muon))
      Event(pass_muons,event.sampleID)
    }

    // select events with at least 2 muons and passing event selections
    val dsDimuonsSel = dsMuonsSel.filter(event => passEventSel(event))

    // compute invariantmass for passing muons
    val dsMll = dsDimuonsSel.map{
      event => Output(invariantMass(event.muons(0),event.muons(1)),event.sampleID)
    }.toDF()

    // create filenames, write parquet files
    //val outputPath = "hdfs:/cms/bigdatasci/olivito/sparktest/"
    val parquetFilenameMll = outputPath + "/dimuonReduced_" + date + "/mll.parquet"
    dsMll.write.format("parquet").save(parquetFilenameMll)

    // stop the session/context
    spark.stop

  } // end of main
}
