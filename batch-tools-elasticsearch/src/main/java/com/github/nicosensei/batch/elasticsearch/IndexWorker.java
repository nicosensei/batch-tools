package com.github.nicosensei.batch.elasticsearch;

import java.util.Collection;
import java.util.Map;

import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.xcontent.XContentBuilder;

import com.github.nicosensei.batch.BatchException;
import com.github.nicosensei.batch.BatchExecutor;
import com.github.nicosensei.batch.Worker;
import com.github.nicosensei.batch.input.InputLine;

/**
 * @author ngiraud
 *
 */
public abstract class IndexWorker<I extends InputLine, D extends IndexableDocument> extends Worker<I> {
	
	BatchExecutor executor = BatchExecutor.getInstance();

	private final TransportClient client;
	
	private final String indexName;
	
	private final String documentTypeName;
	
	protected IndexWorker(
			final IndexBatch<I, D, ? extends IndexWorker<I, D>> batch,
					final String documentTypeName) {
		super(batch.getInputFile(), batch.getBatchState());
		
		this.indexName = batch.getIndexName();
		this.documentTypeName = documentTypeName;
		this.client = batch.getElasticSearchClient();
	}

	@Override
	public IndexBatchState getBatchState() {
		return (IndexBatchState) super.getBatchState();
	}
	
	protected abstract boolean canIngest(I line) throws DocumentBuildingException;

	@Override
	protected void processLine(I line) throws DocumentBuildingException {
		
		if (!canIngest(line)) {
            return;
        }
	
		buildBulkElement(line);
	}
	
	protected abstract void buildBulkElement(I line) throws DocumentBuildingException;
	
	protected abstract Collection<D> getBulkContents();
	
	protected abstract void cleanBulkContents() throws BatchException;
	
	@Override
	protected void sectionComplete() throws BatchException {
		
		Collection<D> bulkContents = getBulkContents();
		
		if (bulkContents.isEmpty()) {
			return;
		}		
		
		BulkRequestBuilder bulk = client.prepareBulk();
		for (D doc : bulkContents) {
			IndexRequestBuilder indexReqBuilder = client.prepareIndex(
                    indexName,
                    documentTypeName,
                    doc.getDocumentId()); 
			switch (getDocumentSourceType()) {
				case documentAsMap: 
					indexReqBuilder.setSource(getDocumentAsMap(doc));
					break;
				case xContentBuilder:
					indexReqBuilder.setSource(getDocumentContentBuilder(doc));
					break;
			}
			bulk.add(indexReqBuilder);
		}
		
		IndexBatchState state = getBatchState();
		
		try {
			BulkResponse bulkResponse = bulk.execute().actionGet();
			if (bulkResponse.hasFailures()) {
				executor.logWarning(bulkResponse.buildFailureMessage());
				for (BulkItemResponse itemResp : bulkResponse.getItems()) {
					if (itemResp.isFailed()) {
						if (state.getLinesSkipped() >= IndexBatch.SKIP_LIMIT) {
							throw new SkipLimitExceededException(IndexBatch.SKIP_LIMIT);
						}
						state.notifyLineSkipped();
						executor.logWarning("Skipped item " + itemResp.toString());
					}
				}
			}
		} finally {
			cleanBulkContents();
		}
	}

	@Override
	protected void jobComplete() {
		
	}
	
	public enum DocumentSourceType {
		documentAsMap,
		xContentBuilder
	}
	
	protected abstract DocumentSourceType getDocumentSourceType();
	
	protected abstract Map<String, Object> getDocumentAsMap(D doc) throws BatchException;
	
	protected abstract XContentBuilder getDocumentContentBuilder(D doc) throws BatchException;
	
}
