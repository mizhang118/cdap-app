package com.ericsson.pm.c26.cdap;

import co.cask.cdap.api.workflow.AbstractWorkflow;

public class C26ModelTrainWorkflow extends AbstractWorkflow {

	  @Override
	  public void configure() {
	      setName(C26AnalyticsApp.WORKFLOW_MODEL_TRAIN);
	      setDescription("Workflow that runs Spark ML org.apache.spark.mllib.fpm.AssociationRules");
	      addSpark("SparkAssociationRule");
	  }
	}
