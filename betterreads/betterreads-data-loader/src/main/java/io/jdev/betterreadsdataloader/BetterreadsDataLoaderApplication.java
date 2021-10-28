package io.jdev.betterreadsdataloader;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import java.util.List;
import java.util.ArrayList;
//import java.util.AbstractList;

import javax.annotation.PostConstruct;

import org.json.JSONArray;
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
import io.jdev.betterreadsdataloader.book.Book;
import io.jdev.betterreadsdataloader.book.BookRepository;

@SpringBootApplication
@EnableConfigurationProperties(DataStaxAstraProperties.class)
public class BetterreadsDataLoaderApplication {

	@Autowired
	AuthorRepository authorRepository;

	@Autowired
	BookRepository bookRepository;

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
		try (Stream<String> lines = Files.lines(path)) {
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

		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	// init works values into cassandra db
	private void initWorks() {

		Path path = Paths.get(worksDumpLocation);
		DateTimeFormatter dateFormat = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSSSS");

		try (Stream<String> lines = Files.lines(path)) {
			lines.limit(40).forEach(line -> {
				String jsonString = line.substring(line.indexOf("{"));
				try {
					JSONObject jsonObject = new JSONObject(jsonString);
					// creates book object
					Book book = new Book();
					book.setId(jsonObject.getString("key").replace("/works/", ""));

					book.setName(jsonObject.optString("title"));
					JSONObject descriptionObject = jsonObject.optJSONObject("description");
					if (descriptionObject != null) {
						book.setDescription(descriptionObject.optString("value"));
					}
					JSONObject publishedObj = jsonObject.optJSONObject("created");
					if (publishedObj != null) {
						String dateStr = publishedObj.getString("value");
						book.setPublishedDate(LocalDate.parse(dateStr, dateFormat));
					}

					JSONArray coversJSONArray = jsonObject.optJSONArray("covers");
					if (coversJSONArray != null) {
						List<String> coverIds = new ArrayList<>();
						for (int i = 0; i < coversJSONArray.length(); i++) {
							coverIds.add(coversJSONArray.getString((i)));
						}
						book.setCoverIds(coverIds);
					}

					JSONArray authorsJSONArray = jsonObject.optJSONArray("authors");
					if (authorsJSONArray != null) {
						List<String> authorIds = new ArrayList<>();
						for (int i = 0; i < authorsJSONArray.length(); i++) {
							String authorId = authorsJSONArray.getJSONObject(i).getJSONObject("author").getString("key")
									.replace("/authors/", "");
							authorIds.add(authorId);
						}
						book.setAuthorIds(authorIds);
						List<String> authorNames = authorIds.stream().map(id -> authorRepository.findById(id))
								.map(optionalAuthor -> {
									if (!optionalAuthor.isPresent())
										return "Unknown Author";
									return optionalAuthor.get().getName();
								}).collect(Collectors.toList());
						book.setAuthorNames(authorNames);
						// Persist using Repository
						bookRepository.save(book);
					}

				} catch (Exception e) {
					e.printStackTrace();
				}
			});

		} catch (IOException e) {
			e.printStackTrace();
		}

	}

	// method runs on application start-up
	@PostConstruct
	public void start() {
		// initAuthors();
		initWorks();
	}

	@Bean
	public CqlSessionBuilderCustomizer sessionBuilderCustomizer(DataStaxAstraProperties astraProperties) {
		Path bundle = astraProperties.getSecureConnectBundle().toPath();
		return builder -> builder.withCloudSecureConnectBundle(bundle);

	}

}
