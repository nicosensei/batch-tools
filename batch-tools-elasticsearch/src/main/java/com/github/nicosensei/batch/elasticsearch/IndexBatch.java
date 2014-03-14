package com.github.nicosensei.batch.elasticsearch;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;

import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;

import com.github.nicosensei.batch.Batch;
import com.github.nicosensei.batch.BatchException;
import com.github.nicosensei.batch.BatchExecutor;
import com.github.nicosensei.batch.input.InputFileException;
import com.github.nicosensei.batch.input.InputLine;

/**
 * @author ngiraud
 *
 */
public abstract class IndexBatch<
I extends InputLine, D extends IndexableDocument, W extends IndexWorker<I, D>> 
extends Batch<I, W> {

	private static final String ES_PING_TIMEOUT = BatchExecutor.getInstance().getProperty(
			IndexBatch.class, "esPingTimeout");

	private static final String[] ES_NODES_ADRESSES = BatchExecutor.getInstance().getProperty(
			IndexBatch.class, "esNodesAdresses").split(",");

	private static final String ES_CLUSTER_NAME = BatchExecutor.getInstance().getProperty(
			IndexBatch.class, "esClusterName");

	protected static final int SKIP_LIMIT = BatchExecutor.getInstance().getIntProperty(
			IndexBatch.class, "skipLimit", 1000);

	private TransportClient client;

	@Override
	protected abstract W workerFactory() throws BatchException;

	@Override
	protected IndexBatchState batchStateFactory() throws BatchException {
		IndexBatchState p = new IndexBatchState(getInputFilePath());
		getInputFile().addCooldownListener(p);
		return p;
	}

	@Override
	public IndexBatchState getBatchState() {
		return (IndexBatchState) super.getBatchState();
	}

	/**
	 * @return the client
	 */
	 protected final TransportClient getElasticSearchClient() {
		 return client;
	 }

	 /**
	  * @return the indexName
	  */
	 protected abstract String getIndexName();

	 @Override
	 protected abstract BatchInputReader<I> inputFileReaderFactory()
			 throws InputFileException;

	 @Override
	 public BatchInputReader<I> getInputFile() {
		 return (BatchInputReader<I>) super.getInputFile();
	 }

	 protected abstract String getInputFilePath();

	 protected abstract void specificInit(String[] args) throws BatchException;

	 @Override
	 protected final void init(String[] args) throws BatchException {

		 specificInit(args);

		 BatchExecutor executor = BatchExecutor.getInstance();

		 Map<String, String> clientSettings = new HashMap<String, String>();
		 clientSettings.put("cluster.name", ES_CLUSTER_NAME);
		 if (ES_PING_TIMEOUT != null && !ES_PING_TIMEOUT.isEmpty()) {
			 clientSettings.put("client.transport.ping_timeout", ES_PING_TIMEOUT);
		 }

		 executor.logInfo("Cluster name is '" + ES_CLUSTER_NAME + "'");
		 this.client = new TransportClient(ImmutableSettings.settingsBuilder().put(clientSettings));

		 for (String esta : ES_NODES_ADRESSES) {
			 String[] parts = esta.split(":");
			 this.client.addTransportAddress(new InetSocketTransportAddress(
					 parts[0], Integer.parseInt(parts[1])));
			 executor.logInfo("Registered node address " + esta);        	
		 }

		 executor.logInfo("Target index name is " + getIndexName());

		 if (initIndex()) {
			 if (indexExists()) {
				 executor.logInfo("Found existing index, will delete it.");
				 deleteIndex();
			 }
			 createIndex();
			 createMappings();
		 }
	 }

	 @Override
	 protected void onComplete() throws BatchException {
		 BatchExecutor executor = BatchExecutor.getInstance();

		 executor.logInfo("Will shut down ElasticSearch client...");
		 this.client.close();
		 executor.logInfo("... done!");

		 IndexBatchState state = getBatchState();
		 StringBuffer summary = new StringBuffer();
		 summary.append("\n\t------------------------------------------------------------");
		 summary.append("\n\t Summary");
		 summary.append("\n\t------------------------------------------------------------");
		 summary.append("\n\t- " + state.getProcessedLines() + " lines processed.");
		 summary.append("\n\t- " + state.getErrors().length + " lines failed.");
		 summary.append("\n\t- " + state.getLinesSkipped() + " lines skipped.");
		 summary.append("\n\t------------------------------------------------------------");
		 BatchExecutor.getInstance().logInfo(summary.toString());
	 }

	 /**
	  * If true, if an index with the defined name exists ins the defined cluster, it will be deleted. 
	  * The index will be created.
	  * @return true if the index should be created, and dropped beforehand if it exists.
	  */
	 protected abstract boolean initIndex();

	 /**
	  * @return true if the target index exists, false otherwise.
	  */
	 private boolean indexExists() {
		 IndicesExistsResponse resp = getElasticSearchClient().admin().indices().prepareExists(getIndexName()).get();
		 return resp.isExists();
	 }

	 private void deleteIndex() throws IndexDeletionFailedException {
		 DeleteIndexResponse resp = getElasticSearchClient().admin().indices().prepareDelete(getIndexName()).get();
		 if (!resp.isAcknowledged()) {
			 throw new IndexDeletionFailedException(getIndexName());
		 }
		 BatchExecutor.getInstance().logInfo("Deleted index '" + getIndexName() + "'");
	 }

	 protected abstract String getIndexSettingsJsonPath();

	 private void createIndex() throws IndexCreationFailedException {
		 String indexName = getIndexName();
		 try {
			 CreateIndexResponse resp = getElasticSearchClient().admin().indices()
					 .prepareCreate(indexName)
					 .setSettings(readTextFile(
							 IndexBatch.class.getResourceAsStream(getIndexSettingsJsonPath()))
							 .toString())
					 .get();
			 if (!resp.isAcknowledged()) {
				 throw new IndexCreationFailedException(getIndexName());
			 }
			 BatchExecutor.getInstance().logInfo("Created index '" + indexName + "'");
		 } catch (final IOException e) {
			 throw new IndexCreationFailedException(indexName, e);
		 } catch (final ElasticSearchException e) {
			 throw new IndexCreationFailedException(indexName, e);
		 }		 
	 }

	 protected abstract String[] getMappingJsonPaths();

	 private void createMappings() throws MappingCreationFailedException {
		 BatchExecutor exec = BatchExecutor.getInstance();
		 String indexName = getIndexName();
		 for (String mappingSource : getMappingJsonPaths()) {
			 // Add the mapping to the index
			 String fileName = new File(mappingSource).getName();
			 String docType = fileName.substring(0, fileName.lastIndexOf(".json"));
			 try {
				 PutMappingResponse resp = client.admin().indices().preparePutMapping(indexName)
						 .setType(docType)
						 .setSource(readTextFile(
								 IndexBatch.class.getResourceAsStream(mappingSource)).toString())
						 .get();
				 if (!resp.isAcknowledged()) {
					 throw new MappingCreationFailedException(indexName, docType);
				 }
				 exec.logInfo("Created mapping '" + docType + "'");
			 } catch (final IOException e) {
				 throw new MappingCreationFailedException(indexName, docType, e);
			 } catch (final ElasticSearchException e) {
				 throw new MappingCreationFailedException(indexName, docType, e);
			 }				
		 }
	 }

	 /**
	  * Reads a text file from the given input stream.
	  * @param inStream the input stream to read from.
	  * @return a {@link StringBuilder} populated with the contents.
	  * @throws IOException if an errors occurs
	  */
	 public static final StringBuilder readTextFile(final InputStream inStream) throws IOException {		
		 StringBuilder sb = new StringBuilder(1000);
		 BufferedReader br = new BufferedReader(new InputStreamReader(inStream));
		 String line = null;
		 while ((line = br.readLine()) != null) {
			 sb.append(line + "\n");
		 }
		 br.close();
		 return sb;
	 }

}
