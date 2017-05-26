package com.ericsson.pm.c26.spark

import com.ericsson.pm.c26.cdap.C26AnalyticsApp
import co.cask.cdap.api.spark.{SparkExecutionContext, SparkMain}
import org.apache.spark.SparkContext
import org.apache.spark.mllib.fpm.{AssociationRules, FPGrowth, FPGrowthModel}
import org.apache.spark.rdd.RDD
import org.slf4j.{Logger, LoggerFactory}

class AssociationRule extends SparkMain {
  import AssociationRuleProgram._
  
  override def run(implicit sec: SparkExecutionContext) {
    val sc = new SparkContext

    LOG.info("Processing featuer data using association rule")

    val featuresForTrainning: RDD[(String, String)] = sc.fromDataset(C26AnalyticsApp.DATASET_TRAIN_STORE)
    val vins = featuresForTrainning.keys;
    vins.foreach { x => LOG.info("Keys for training: {}", x) }

    LOG.info("Done!")
  }
}

object AssociationRuleProgram {
  final val LOG: Logger = LoggerFactory.getLogger(classOf[AssociationRule])
}