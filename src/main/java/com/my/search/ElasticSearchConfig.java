package com.my.search;

public enum ElasticSearchConfig {
	INDEX("iasksearch"), TYPE("fulltext"), AUTOCOMPLETION("autocompletion");
	private String name;

	private ElasticSearchConfig(String name) {
		this.name = name;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}
}
