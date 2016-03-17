package com.my.search;

import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;

public class SimpleElastic {
	private String clusterName;
	private String nodeAdrees;// host,host2

	private Client client;
	private TransportClient tClient;

	public String getClusterName() {
		return clusterName;
	}

	public void setClusterName(String clusterName) {
		this.clusterName = clusterName;
	}

	public String getNodeAdrees() {
		return nodeAdrees;
	}

	public void setNodeAdrees(String nodeAdrees) {
		this.nodeAdrees = nodeAdrees;
	}

	public Client getClient() {
		if (null != client) {
			return client;
		}

		buildClient();
		return client;
	}

	private void buildClient() {
		Settings settings = ImmutableSettings.settingsBuilder()
				.put("cluster.name", this.clusterName).build();
		tClient = new TransportClient(settings);

		String[] hosts = nodeAdrees.split(",");

		for (String host : hosts) {
			tClient = tClient
					.addTransportAddress(new InetSocketTransportAddress(host,
							9300));// 固定端口为9300
		}
		client = tClient;
	}

	public void destroy() {
		if (null != tClient) {
			tClient.close();
		}
		if (null != client) {
			client.close();
		}
	}
}
