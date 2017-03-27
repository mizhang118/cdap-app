package com.ericsson.pm.analytics;

import java.util.ArrayList;
import java.util.List;

public class AprioriResult {

	private List<AprioriRule> rules;

	public AprioriResult(List<String> content) {
		rules = new ArrayList<AprioriRule>();
		loadFromOutputContent(content);
	}

	private void loadFromOutputContent(List<String> content) {
		int startIndex = getAprioriContentRuleStartIndex(content);
		for (int i=startIndex; i<content.size()-10; i++) {
			String ruleText = content.get(i);
			while (!ruleText.contains("=>")) {
				i++;
				if(content.get(i)!=null)
					ruleText += content.get(i);
			}
			rules.add(new AprioriRule(ruleText));
		}
	}

	private static int getAprioriContentRuleStartIndex(List<String> content) {
		int index;
		for (index=0; index<content.size(); index++) {
			if (content.get(index).trim().startsWith("lhs")) break;
		}
		return ++index;
	}

	public List<AprioriRule> getRules() { return rules; }

	public void setRules(List<AprioriRule> rules) { this.rules = rules; }

}

