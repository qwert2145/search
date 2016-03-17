package com.my.search;

import com.my.model.Field;
import com.my.model.Question;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.count.CountResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.suggest.SuggestRequestBuilder;
import org.elasticsearch.action.suggest.SuggestResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.Requests;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.*;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.highlight.HighlightField;
import org.elasticsearch.search.suggest.Suggest.Suggestion.Entry.Option;
import org.elasticsearch.search.suggest.completion.CompletionSuggestionBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ExecutionException;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

@Service
public class ElasticUtil {
	@Autowired
	private SimpleElastic simpleElastic;

	private static final Logger logger = LoggerFactory
			.getLogger(ElasticUtil.class);

	public void indexAllQuestions(List<Question> questions) throws IOException {
		logger.debug("Indexing bulk data request, for size:" + questions.size());
		if (questions.isEmpty()) {
			return;
		}
		List<IndexRequestBuilder> requests = new ArrayList<IndexRequestBuilder>();

		for (Question question : questions) {
			try {
				requests.add(getIndexRequestBuilderForAQuestion(question));
			} catch (Exception ex) {
				logger.error(
						"Error occurred while creating index document for question with id: "
								+ question.getId()
								+ ", moving to next question!", ex);
			}
		}
		processBulkRequests(requests);
	}

	protected BulkResponse processBulkRequests(
			List<IndexRequestBuilder> requests) throws IOException {
		if (requests.size() > 0) {
			BulkRequestBuilder bulkRequest = simpleElastic.getClient()
					.prepareBulk();

			for (IndexRequestBuilder indexRequestBuilder : requests) {
				bulkRequest.add(indexRequestBuilder);
			}

			logger.debug("Executing bulk index request for size:"
					+ requests.size());
			BulkResponse bulkResponse = bulkRequest.execute().actionGet();

			logger.debug("Bulk operation data index response total items is:"
					+ bulkResponse.getItems().length);
			if (bulkResponse.hasFailures()) {
				// process failures by iterating through each bulk response item
				logger.error("bulk operation indexing has failures:"
						+ bulkResponse.buildFailureMessage());
			}
			return bulkResponse;
		} else {
			logger.debug("Executing bulk index request for size: 0");
			return null;
		}
	}

	private IndexRequestBuilder getIndexRequestBuilderForAQuestion(
			Question question) throws IOException {
		XContentBuilder contentBuilder = getXContentBuilderForAQuestion(question);

		IndexRequestBuilder indexRequestBuilder = simpleElastic.getClient()
				.prepareIndex(ElasticSearchConfig.INDEX.getName(),
						ElasticSearchConfig.TYPE.getName(),
						String.valueOf(question.getId()));

		indexRequestBuilder.setSource(contentBuilder);

		return indexRequestBuilder;
	}

	private XContentBuilder getXContentBuilderForAQuestion(Question question)
			throws IOException {
		XContentBuilder contentBuilder = null;
		try {
			contentBuilder = jsonBuilder().prettyPrint().startObject();

			contentBuilder
					.field(Field.ID.getFieldName(), question.getId())
					.field(Field.TYPEID.getFieldName(), question.getTypeId())
					.field(Field.CONTENT.getFieldName(), question.getContent())
					.field(Field.CREATIONDATE.getFieldName(),
							question.getCreationDate())
					.field(Field.STATUS.getFieldName(), question.getStatus());
			contentBuilder.endObject();
		} catch (IOException ex) {
			logger.error(ex.getMessage());
			throw new RuntimeException(
					"Error occured while creating question gift json document!",
					ex);
		}

		logger.debug("Generated XContentBuilder for document id {} is {}",
				new Object[] { question.getId(),
						contentBuilder.prettyPrint().string() });

		return contentBuilder;
	}

	public boolean deleteIndex(String indexName) {
		return simpleElastic.getClient().admin().indices()
				.prepareDelete(indexName).execute().actionGet()
				.isAcknowledged();
	}

	public void deleteQuestion(long questionId) {
		simpleElastic
				.getClient()
				.prepareDelete(ElasticSearchConfig.INDEX.getName(),
						ElasticSearchConfig.TYPE.getName(),
						String.valueOf(questionId)).get();
	}

	public List<Map<String, Object>> searchQuestion(String query, long typeId,
			int startPos, int pageSize) {
		AndFilterBuilder andFilterBuilder = FilterBuilders.andFilter();
		andFilterBuilder.add(FilterBuilders.termFilter("typeId", typeId));
		SearchResponse searchResponse = null;
		if (typeId == 0) {
			searchResponse = simpleElastic
					.getClient()
					.prepareSearch(ElasticSearchConfig.INDEX.getName())
					.setTypes(ElasticSearchConfig.TYPE.getName())
					.setQuery(
							QueryBuilders.multiMatchQuery(query, "content",
									"content.cn", "content.en"))
					.addHighlightedField("content")
					.setHighlighterPreTags("<font color=\"red\">")
					.setHighlighterPostTags("</font>").setFrom(startPos)
					.setSize(pageSize).execute().actionGet();
		} else {
			searchResponse = simpleElastic
					.getClient()
					.prepareSearch(ElasticSearchConfig.INDEX.getName())
					.setTypes(ElasticSearchConfig.TYPE.getName())
					.setQuery(
							QueryBuilders.filteredQuery(QueryBuilders
									.multiMatchQuery(query, "content",
											"content.cn", "content.en"),
									andFilterBuilder))
					.addHighlightedField("content")
					.setHighlighterPreTags("<font color=\"red\">")
					.setHighlighterPostTags("</font>").setFrom(startPos)
					.setSize(pageSize).execute().actionGet();
		}

		SearchHits hits = searchResponse.getHits();
		List<Map<String, Object>> list = new ArrayList<Map<String, Object>>();
		for (SearchHit hit : hits.getHits()) {
			Map<String, Object> map = new HashMap<String, Object>();
			map.put("id", hit.getSource().get("id"));

			Map<String, HighlightField> result = hit.highlightFields();
			HighlightField titleField = result.get("content");
			Text[] titleTexts = null;
			try {
				titleTexts = titleField.fragments();
				for (Text text : titleTexts) {
					map.put("content", text.toString());
				}
			} catch (NullPointerException n) {
				map.put("content", hit.getSource().get("content"));
			}

			list.add(map);
		}
		return list;
	}

	public long getSearchCount(String query, long typeId) {
		AndFilterBuilder andFilterBuilder = FilterBuilders.andFilter();
		andFilterBuilder.add(FilterBuilders.termFilter("typeId", typeId));
		CountResponse countResponse = null;
		if (typeId == 0) {
			countResponse = simpleElastic
					.getClient()
					.prepareCount(ElasticSearchConfig.INDEX.getName())
					.setTypes(ElasticSearchConfig.TYPE.getName())
					.setQuery(
							QueryBuilders.multiMatchQuery(query, "content",
									"content.cn", "content.en")).execute()
					.actionGet();
		} else {
			countResponse = simpleElastic
					.getClient()
					.prepareCount(ElasticSearchConfig.INDEX.getName())
					.setTypes(ElasticSearchConfig.TYPE.getName())
					.setQuery(
							QueryBuilders.filteredQuery(QueryBuilders
									.multiMatchQuery(query, "content",
											"content.cn", "content.en"),
									andFilterBuilder)).execute().actionGet();
		}
		return countResponse.getCount();
	}

	public void createMapping() throws IOException {
		// create a index
		simpleElastic.getClient().admin().indices()
				.prepareCreate(ElasticSearchConfig.INDEX.getName()).execute()
				.actionGet();
		// create a mapping
		XContentBuilder mapping = XContentFactory.jsonBuilder().startObject()
				.startObject(ElasticSearchConfig.TYPE.getName())
				.startObject("properties").startObject("id")
				.field("type", "long").field("index", "not_analyzed")
				.endObject().startObject("typeId")
				.field("type", "long")
				.field("index", "not_analyzed")
				.endObject()
				.startObject("status")
				.field("type", "integer")
				.field("index", "not_analyzed")
				.endObject()
				.startObject("content")
				.field("type", "completion")
				// .field("index", "analyzed")
				.startObject("fields").startObject("cn")
				.field("type", "string").field("analyzer", "ik").endObject()
				.startObject("en").field("type", "string")
				.field("analyzer", "english").endObject().endObject()
				.endObject().startObject("creationDate").field("type", "date")
				.field("index", "not_analyzed").endObject().endObject()
				.endObject();

		PutMappingRequest mappingRequest = Requests
				.putMappingRequest(ElasticSearchConfig.INDEX.getName())
				.type(ElasticSearchConfig.TYPE.getName()).source(mapping);
		simpleElastic.getClient().admin().indices().putMapping(mappingRequest)
				.actionGet();
	}

	public void createQuestion(Question question)
			throws ElasticsearchException, IOException {
		getIndexRequestBuilderForAQuestion(question).get();
	}

	public void updateQuestion(long questionId, long typeId, int status)
			throws ElasticsearchException, IOException, InterruptedException,
			ExecutionException {
		XContentBuilder contentBuilder = null;
		contentBuilder = jsonBuilder().prettyPrint().startObject();
		contentBuilder.field(Field.TYPEID.getFieldName(), typeId).field(
				Field.STATUS.getFieldName(), status);
		contentBuilder.endObject();

		UpdateRequest updateRequest = new UpdateRequest();
		updateRequest.index(ElasticSearchConfig.INDEX.getName());
		updateRequest.type(ElasticSearchConfig.TYPE.getName());
		updateRequest.id(String.valueOf(questionId));
		updateRequest.doc(contentBuilder);
		simpleElastic.getClient().update(updateRequest).get();
	}

	public List<String> autoComplete(String queryString) {
		CompletionSuggestionBuilder suggesBuilder = new CompletionSuggestionBuilder(
				ElasticSearchConfig.AUTOCOMPLETION.getName())
				.field(Field.CONTENT.getFieldName())
				// .analyzer("standard")
				.size(10).text(queryString);
		SuggestRequestBuilder addSuggestion = simpleElastic.getClient()
				.prepareSuggest(ElasticSearchConfig.INDEX.getName())
				.addSuggestion(suggesBuilder);
		SuggestResponse suggestResponse = addSuggestion.get();
		List<String> suggestions = new ArrayList<String>();

		if (suggestResponse != null
				&& suggestResponse.getSuggest() != null
				&& suggestResponse.getSuggest().getSuggestion(
						ElasticSearchConfig.AUTOCOMPLETION.getName()) != null) {
			for (org.elasticsearch.search.suggest.Suggest.Suggestion.Entry<? extends Option> suggestEntry : suggestResponse
					.getSuggest()
					.getSuggestion(ElasticSearchConfig.AUTOCOMPLETION.getName())
					.getEntries()) {
				for (Option option : suggestEntry.getOptions()) {
					suggestions.add(option.getText().string());
					// logger.warn(option.getText().string());
				}
			}
		}
		return suggestions;
	}

	public boolean isIndexExists(String indexName) {
		return simpleElastic.getClient().admin().indices()
				.prepareExists(indexName).get().isExists();
	}

	public Set<Integer> getTypeIds(String query) {
		SearchResponse searchResponse = null;
		searchResponse = simpleElastic
				.getClient()
				.prepareSearch(ElasticSearchConfig.INDEX.getName())
				.setTypes(ElasticSearchConfig.TYPE.getName())
				.setQuery(
						QueryBuilders.multiMatchQuery(query, "content",
								"content.cn", "content.en")).execute()
				.actionGet();
		SearchHits hits = searchResponse.getHits();
		Set<Integer> typeIds = new HashSet<Integer>();
		for (SearchHit hit : hits.getHits()) {
			typeIds.add((Integer) hit.getSource().get("typeId"));
		}
		return typeIds;
	}

	public List<Map<String, Object>> searchQuestionNew(String query,
			List<Long> typeIds, boolean isResolved, int startPos, int pageSize) {
		OrFilterBuilder orFilterList = FilterBuilders.orFilter();
		for (Long typeId : typeIds) {
			FilterBuilder filterBuilder = FilterBuilders.termFilter("typeId",
					typeId);
			orFilterList.add(filterBuilder);
		}

		QueryBuilder qb = null;
		if (isResolved) {
			qb = QueryBuilders
					.boolQuery()
					.must(QueryBuilders.multiMatchQuery(query, "content",
							"content.cn", "content.en"))
					.must(QueryBuilders.termQuery("status", 3));
		} else {
			qb = QueryBuilders
					.boolQuery()
					.must(QueryBuilders.multiMatchQuery(query, "content",
							"content.cn", "content.en"))
					.mustNot(QueryBuilders.termQuery("status", 3));
		}

		SearchResponse searchResponse = simpleElastic.getClient()
				.prepareSearch(ElasticSearchConfig.INDEX.getName())
				.setTypes(ElasticSearchConfig.TYPE.getName())
				.setQuery(QueryBuilders.filteredQuery(qb, orFilterList))
				.addHighlightedField("content")
				.addHighlightedField("content.cn")
				.addHighlightedField("content.en")
				.setHighlighterPreTags("<font color=\"red\">")
				.setHighlighterPostTags("</font>").setFrom(startPos)
				.setSize(pageSize).execute().actionGet();

		SearchHits hits = searchResponse.getHits();
		List<Map<String, Object>> list = new ArrayList<Map<String, Object>>();
		for (SearchHit hit : hits.getHits()) {
			Map<String, Object> map = new HashMap<String, Object>();
			map.put("id", hit.getSource().get("id"));

			Map<String, HighlightField> result = hit.highlightFields();
			HighlightField titleField = result.get("content.en");
			Text[] titleTexts = null;
			try {
				titleTexts = titleField.fragments();
				for (Text text : titleTexts) {
					map.put("content", text.toString());
				}
			} catch (NullPointerException n) {
				map.put("content", hit.getSource().get("content"));
			}

			list.add(map);
		}
		return list;
	}

	public long getSearchCountNew(String query, List<Long> typeIds,
			boolean isResolved) {
		QueryBuilder qb = null;
		if (isResolved) {
			qb = QueryBuilders
					.boolQuery()
					.must(QueryBuilders.multiMatchQuery(query, "content",
							"content.cn", "content.en"))
					.must(QueryBuilders.termQuery("status", 3));
		} else {
			qb = QueryBuilders
					.boolQuery()
					.must(QueryBuilders.multiMatchQuery(query, "content",
							"content.cn", "content.en"))
					.mustNot(QueryBuilders.termQuery("status", 3));
		}

		OrFilterBuilder orFilterList = FilterBuilders.orFilter();
		for (Long typeId : typeIds) {
			FilterBuilder filterBuilder = FilterBuilders.termFilter("typeId",
					typeId);
			orFilterList.add(filterBuilder);
		}

		CountResponse countResponse = null;
		countResponse = simpleElastic.getClient()
				.prepareCount(ElasticSearchConfig.INDEX.getName())
				.setTypes(ElasticSearchConfig.TYPE.getName())
				.setQuery(QueryBuilders.filteredQuery(qb, orFilterList))
				.execute().actionGet();
		return countResponse.getCount();
	}
}
