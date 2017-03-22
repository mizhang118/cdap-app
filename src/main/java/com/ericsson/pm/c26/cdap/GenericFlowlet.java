package com.ericsson.pm.c26.cdap;

import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import co.cask.cdap.api.annotation.Tick;
import co.cask.cdap.api.flow.flowlet.AbstractFlowlet;

public class GenericFlowlet extends AbstractFlowlet {
	private static final Logger LOG = LoggerFactory.getLogger(GenericFlowlet.class);
	
	/**
	 * do audit every minute
	 * 
	 * @throws InterruptedException
	 */
	@Tick(delay = 60000L, unit = TimeUnit.MILLISECONDS)
	public void audit() throws InterruptedException {
		this.getContext().getInstanceId();
	}
}
