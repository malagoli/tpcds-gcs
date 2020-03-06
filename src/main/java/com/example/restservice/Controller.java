package com.example.restservice;

import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.cloud.storage.*;
import com.teradata.tpcds.*;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;


@RestController
public class Controller {

	@GetMapping("/run")
	public String run(
                    @RequestParam(value = "scale", defaultValue = "1") String scale,
                    @RequestParam(value = "bucket") String bucketName
					  ) throws IOException {




		List<Table> tablesToGenerate = Table.getBaseTables();
		Options options = new Options();
		options.directory = "/tmp";
		options.overwrite = true;

		options.scale = Double.parseDouble(scale);

		Session session = options.toSession();


		AtomicInteger currentTableCount = new AtomicInteger();

				TableGenerator tableGenerator = new TableGenerator(session.withChunkNumber(1));

				tablesToGenerate.forEach( table -> {
					currentTableCount.getAndIncrement();
					System.out.println("Starting generation of table " + table + " (" + currentTableCount + " of " + tablesToGenerate.size() + ")" );
					tableGenerator.generateTable(table);
					System.out.println("Ended generation of table " + table);
					System.out.println("-----------------------------------------------------------------");
				});


		AtomicInteger currentFileCount = new AtomicInteger();
		AtomicBoolean uploadError = new AtomicBoolean(false);
		Storage storage = StorageOptions.getDefaultInstance().getService();

        Files.newDirectoryStream(Paths.get("/tmp"),
                path -> path.toString().endsWith(".dat"))
                .forEach( f -> {
                    try {
                    	currentFileCount.getAndIncrement();
						System.out.println("Starting uploading of file " + f.toString() + " (" + currentFileCount + " of " + tablesToGenerate.size() + ")" );
						uploadFile(f, storage, bucketName);
						System.out.println("Ended generation of table " + f.toString());
						System.out.println("-----------------------------------------------------------------");
                    } catch (IOException e) {
						e.printStackTrace();
                    	uploadError.set(true);
                    }
                });

        if(uploadError.get()) {
        	return "Error in uploading files to GCS";
		}

		return "Successfully created " + currentTableCount + " files on " + bucketName;
	}

	private static void uploadFile(Path path, Storage storage, String bucketName) throws IOException {
        String fileName = path.toString();

	    FileInputStream fileStream=new FileInputStream(fileName);

        DateTimeFormatter dtf = DateTimeFormat.forPattern("-YYYY-MM-dd-HHmmssSSS");
        DateTime dt = DateTime.now(DateTimeZone.UTC);
        String dtString = dt.toString(dtf);

        storage.create(
                BlobInfo
                        .newBuilder(bucketName, fileName + dtString )
                        .build(),
                fileStream
        );

        fileStream.close();
    }
}
