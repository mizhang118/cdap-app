package com.ericsson.pm.c26.spark

import com.ericsson.pm.c26.cdap.C26AnalyticsApp
import com.ericsson.pm.c26.cdap.C26TripDataset
import com.ericsson.pm.c26.entities.VolvoModel;

import co.cask.cdap.api.TxRunnable
import co.cask.cdap.api.common.Bytes
import co.cask.cdap.api.data.DatasetContext
import co.cask.cdap.api.spark.{SparkExecutionContext, SparkMain}

import java.util.List;

import org.apache.spark.SparkContext
import org.apache.spark.mllib.fpm.{AssociationRules, FPGrowth, FPGrowthModel}
import org.apache.spark.mllib.fpm.FPGrowth.FreqItemset
import org.apache.spark.rdd.RDD
import org.slf4j.{Logger, LoggerFactory}

class AssociationRule extends SparkMain {
  import AssociationRuleProgram._
  
  override def run(implicit sec: SparkExecutionContext) {
    LOG.info("Processing feature data using association rule")
    val sc = new SparkContext

    //val featuresForTrainning: RDD[(String, String)] = sc.fromDataset(C26AnalyticsApp.DATASET_TRAIN_STORE)
    //val vins = featuresForTrainning.keys;
    //vins.foreach { x => LOG.info("Keys for training: {}", x) }
    
    sec.execute(new TxRunnable {
      override def run(context: DatasetContext) = {
        val trainStore: C26TripDataset = context.getDataset(C26AnalyticsApp.DATASET_TRAIN_STORE)
        val featureStore: C26TripDataset = context.getDataset(C26AnalyticsApp.DATASET_FEATURE_STORE)
        val modelStore: C26TripDataset = context.getDataset(C26AnalyticsApp.DATASET_MODEL_STORE)
        
        val vins: List[String] = trainStore.getVinForTrain()
        val iter = vins.iterator()
        while( iter.hasNext() ) {
          val vin = iter.next();
          LOG.trace("Detected vin as {}", vin)
          try {
            val features: List[String] = featureStore.getARFeatures(vin)
            val dataCount = features.size();
            LOG.trace("Feature number: {}", dataCount)
            val support: Double = calSupport(dataCount)
            LOG.trace("Use suport {} for training", support);
            
            val featureSeq = scala.collection.JavaConverters.asScalaIteratorConverter(features.iterator).asScala.toSeq
            val rdd = sc.parallelize(featureSeq)
            LOG.info("RDD contains {} records as trip features for training.", rdd.count())
          
            val transactions: RDD[Array[String]] = rdd.map(s => s.trim.split(','))
            LOG.trace("RDD finished map() and has {} records.", transactions.count)
          
            val trans_filter = transactions.filter(e => e(0) != e(1))
            LOG.trace("RDD has {} records after filtering.", trans_filter.count)
          
            val fpg = new FPGrowth().setMinSupport(support).setNumPartitions(10)
            val model = fpg.run(trans_filter)
            val minConfidence = 0.8
            val models = model.generateAssociationRules(minConfidence)
            val outputModelCount = models.count()
            LOG.info("Total model number is {}", outputModelCount)
            if ( outputModelCount > 0 ) {
              var count: Int = 0
              //at first clean dataset
              modelStore.deleteData(vin);
              
              //then save models into dataset
              models.collect().foreach { rule => 
                val antecedent = rule.antecedent.mkString("", ",", "")
                val consequent = rule.consequent.mkString("", ",", "")
                if ( consequent.indexOf("destination=") >= 0 && antecedent.indexOf("origin=") >= 0 ) {
                  count = count + 1
                  val model: VolvoModel = VolvoModel.parse(antecedent, consequent, rule.confidence)
                  modelStore.addData(vin, count.toString(), model.toJson())
                }
                //LOG.info(rule.antecedent.mkString("[", ",", "]") + " => " + rule.consequent.mkString("[", ",", "]") + ", " + rule.confidence) 
              }
              LOG.trace("Model number is {} after filtering", count)
            }
          }
          catch {
            case e: Exception => LOG.error("Exception has been generated when train vin " + vin + ": {}", e.getMessage)
          }
        }
      }
    })

    LOG.info("Done!")
  }
  
  def calSupport(dataCount: Int): Double = {
    var support: Double = 0.2;
    if (dataCount < 4) {
        support = 0.25;
    } else {
        support = 1.01 / (dataCount + 1);
    }
    support
  }
}

object AssociationRuleProgram {
  final val LOG: Logger = LoggerFactory.getLogger(classOf[AssociationRule])
}