package com.njb;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.UUID;

import org.apache.http.HttpHost;
import org.apache.maven.plugin.AbstractMojo;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugin.MojoFailureException;
import org.apache.maven.plugins.annotations.LifecyclePhase;
import org.apache.maven.plugins.annotations.Mojo;
import org.apache.maven.plugins.annotations.Parameter;
import org.apache.maven.project.MavenProject;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.json.JSONArray;

@Mojo(name = "index-json", defaultPhase = LifecyclePhase.VERIFY)
public class IndexCucumberJsonMojo extends AbstractMojo {

	@Parameter(defaultValue = "${project}", required = true, readonly = true)
	MavenProject project;

	@Parameter(property = "inputDirectory")
	String inputDirectory;

	@Parameter(property = "elasticSearchUrl", required = true)
	String elasticSearchUrl;

	@Parameter(property = "indexName", required = true)
	String indexName;

	public void execute() throws MojoExecutionException, MojoFailureException {

		getLog().info("inputDirectory: " + inputDirectory);
		getLog().info("elasticSearchUrl: " + elasticSearchUrl);
		getLog().info("indexName: " + indexName);

		try {
			Files.newDirectoryStream(Paths.get(inputDirectory), path -> path.toString().endsWith(".json"))
					.forEach(jsonFilePath -> {
						JSONArray jsonArray = null;
						try {
							String jsonString = new String(Files.readAllBytes(jsonFilePath), StandardCharsets.UTF_8);
							getLog().info(jsonString);
							jsonArray = new JSONArray(jsonString);
						} catch (IOException e) {
							getLog().error(e.getMessage(), e);
						}

						URL url = null;
						try {
							url = new URL(elasticSearchUrl);
						} catch (MalformedURLException me) {
							getLog().error(me.getMessage(), me);
						}

						RestHighLevelClient client = new RestHighLevelClient(
								RestClient.builder(new HttpHost(url.getHost(), url.getPort(), url.getProtocol())));

						IndexRequest request = new IndexRequest(indexName);
						request.id(UUID.randomUUID().toString());
						// TODO bulk request
						request.source(jsonArray.get(0).toString(), XContentType.JSON);

						try {
							IndexResponse indexResponse = client.index(request, RequestOptions.DEFAULT);
							// TODO indexResponse Listener
							getLog().info("indexResponse: " + indexResponse.getIndex() + " " + indexResponse.getId());

						} catch (IOException e) {
							getLog().error(e.getMessage(), e);
						} finally {
							if (client != null)
								try {
									client.close();
								} catch (IOException e) {
								}
						}

					});

		} catch (IOException e) {
			getLog().error(e.getMessage(), e);
		}

	}
}