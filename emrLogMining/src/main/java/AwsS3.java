import com.amazonaws.auth.SystemPropertiesCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.ListObjectsV2Result;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.zip.GZIPInputStream;

@Slf4j
@ToString
public class AwsS3
{

  private final static AmazonS3 amazonS3 = AmazonS3ClientBuilder.standard()
                                                                .withCredentials(new SystemPropertiesCredentialsProvider())
                                                                .build();

  public static Set<String> listItemsAtPrefix(String prefix)
  {

    prefix = prefix.endsWith("/") ? prefix : prefix.concat("/");

    final Set<String> itemsAtPrefix = new LinkedHashSet<>();

    try {
      URI s3PrefixPathUrl = new URI(prefix);
      final ListObjectsV2Result listObjectsV2Result = amazonS3.listObjectsV2(
          s3PrefixPathUrl.getHost(),
          s3PrefixPathUrl.getPath().substring(1, s3PrefixPathUrl.getPath().length())
      );
      for (S3ObjectSummary s3ObjectSummary : listObjectsV2Result.getObjectSummaries()) {

        //First remove the starting (s3 prefix path the we already know
        final String unkownPartOfKey = s3ObjectSummary.getKey()
                                                      .substring(
                                                          s3PrefixPathUrl.getPath()
                                                                         .substring(
                                                                             1,
                                                                             s3PrefixPathUrl.getPath().length()
                                                                         )
                                                                         .length(),
                                                          s3ObjectSummary.getKey().length()
                                                      );

        if (unkownPartOfKey.contains("/")) {
          itemsAtPrefix.add(unkownPartOfKey.substring(0, unkownPartOfKey.indexOf("/")));
        }

      }

    }
    catch (URISyntaxException e) {
      e.printStackTrace();
    }

    return itemsAtPrefix;

  }

  public static List<S3ObjectSummary> listLeafItemsAtPrefix(String prefix){


    prefix = prefix.endsWith("/") ? prefix : prefix.concat("/");

    ListObjectsV2Result listObjectsV2Result = null;

    try {
      URI s3PrefixPathUrl = new URI(prefix);
      listObjectsV2Result = amazonS3.listObjectsV2(
          s3PrefixPathUrl.getHost(),
          s3PrefixPathUrl.getPath().substring(1, s3PrefixPathUrl.getPath().length())
      );
    }
    catch (URISyntaxException e) {
      log.error("Error in reading leaf nodes at "+prefix);
    }

    return listObjectsV2Result.getObjectSummaries();

  }

  public static List<String> getS3ObjectContents(String path)
  {

    final List<String> lines = new ArrayList<>();

    try {
      URI s3PrefixPathUrl = new URI(path);

      String bucket = s3PrefixPathUrl.getHost();
      String key = s3PrefixPathUrl.getPath().substring(1, s3PrefixPathUrl.getPath().length());

      final S3Object objectToRead = amazonS3.getObject(
          bucket,
          key
      );

      InputStream gzipStream = new GZIPInputStream(objectToRead.getObjectContent());
      Reader decoder = new InputStreamReader(gzipStream);
      BufferedReader bufferedReader = new BufferedReader(decoder);


      String line;

      while (((line = bufferedReader.readLine()) != null)) {
        lines.add(line);
      }

    }
    catch (MalformedURLException e) {
      e.printStackTrace();
    }
    catch (IOException e) {
      e.printStackTrace();
    }
    catch (URISyntaxException e) {
      e.printStackTrace();
    }

    return lines;

  }


  public static List<String> getS3ObjectContents(String bucket, String key)
  {

    final List<String> lines = new ArrayList<>();

    try {

      final S3Object objectToRead = amazonS3.getObject(
          bucket,
          key
      );

      InputStream gzipStream = new GZIPInputStream(objectToRead.getObjectContent());
      Reader decoder = new InputStreamReader(gzipStream);
      BufferedReader bufferedReader = new BufferedReader(decoder);


      String line = null;

      while (((line = bufferedReader.readLine()) != null)) {
        lines.add(line);
      }

    }
    catch (MalformedURLException e) {
      e.printStackTrace();
    }
    catch (IOException e) {
      e.printStackTrace();
    }

    return lines;
  }
}
