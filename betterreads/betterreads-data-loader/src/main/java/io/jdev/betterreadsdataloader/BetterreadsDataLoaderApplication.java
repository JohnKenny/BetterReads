package io.jdev.betterreadsdataloader;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.stream.Stream;

import javax.annotation.PostConstruct;

import org.json.JSONException;
import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.cassandra.CqlSessionBuilderCustomizer;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;

import connection.DataStaxAstraProperties;
import io.jdev.betterreadsdataloader.author.Author;
import io.jdev.betterreadsdataloader.author.AuthorRepository;

@SpringBootApplication    
@EnableConfigurationProperties(DataStaxAstraProperties.class)
public class BetterreadsDataLoaderApplication {

	@Autowired AuthorRepository authorRepository;

	@Value("${datadump.location.author}")
	private String authorDumpLocation;

	@Value("${datadump.location.works}")
	private String worksDumpLocation;

	public static void main(String[] args) {
		SpringApplication.run(BetterreadsDataLoaderApplication.class, args);   
	}

  
	// initialises author values into cassandra db
	public void initAuthors() {
		Path path = Paths.get(authorDumpLocation);
		try (Stream<String> lines = Files.lines(path)){
			lines.limit(4).forEach(line -> {
				// read and parse line
				String jsonString = line.substring(line.indexOf("{"));
				
				try {
					JSONObject jsonObject = new JSONObject(jsonString);
					// construct Author object
					Author author = new Author();
					author.setName(jsonObject.optString("name"));
					author.setPersonalName(jsonObject.optString("personal_name"));
					author.setId((jsonObject.optString("key").replace("/authors/", "")));
					// Persist using Repository
					authorRepository.save(author);

				} catch (JSONException e) {
					
					e.printStackTrace();
				}
				

			});

		} catch(IOException e){
			e.printStackTrace();
		}
	}

	// init works values into cassandra db
	private void initWorks() {
		Path path = Paths.get(worksDumpLocation);
		try(Stream<String> lines = Files.lines(path)){
			lines.forEach(line -> {
				String jsonString = line.substring(line.indexOf("{"));
				try {
					JSONObject jsonObject = new JSONObject(jsonString);  

				} catch(JSONException e){
					e.printStackTrace();
				}
			});

		} catch(IOException e){
			e.printStackTrace();
		}

	}

	// method runs on application start-up
	@PostConstruct
	public void start(){
		initAuthors();
		initWorks();
	}

	



	


	@Bean
	public CqlSessionBuilderCustomizer sessionBuilderCustomizer(DataStaxAstraProperties astraProperties){
		Path bundle = astraProperties.getSecureConnectBundle().toPath();
		return builder -> builder.withCloudSecureConnectBundle(bundle);
		
	}

}
