import com.amazonaws.auth.SystemPropertiesCredentialsProvider;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduce;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduceClientBuilder;
import com.amazonaws.services.elasticmapreduce.model.Cluster;
import com.amazonaws.services.elasticmapreduce.model.DescribeClusterRequest;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.joda.time.DateTime;
import org.json.simple.JSONObject;

import java.util.ArrayList;
import java.util.List;

/**
 * Mine EMR logs when the EMR is being used as an "External YARN", i.e. the jobs are submitted to the EMR via the local spark-submits or map-reduces or spark-shells.
 * In this example, we will mine the logs to calculate the total idle time that is observed on the EMR between subsequent jobs
 */
@Slf4j
@AllArgsConstructor
public class MineEMRLogsV2
{

  @Getter
  private final String clusterId;

  @Getter
  @AllArgsConstructor
  @ToString
  class EmrStep
  {

    final private String stepId;

    final private String mainClass;

    final private Long startTime;

    final private Long endTime;

    final private Long executionTime;

  }

  private List<EmrStep> getSteps()
  {

    log.info("Querying EMR with id "
             + this.getClusterId()
             + " for the steps executed on the EMR by querying yarn logs");

    List<EmrStep> emrStepList = new ArrayList<>();

    final Cluster cluster = getAmazonElasticMapReduceClient().describeCluster(new DescribeClusterRequest().withClusterId(
        this.getClusterId()))
                                                             .getCluster();

    try {
      log.debug("Sleeping for one second to throttle call rate.");
      Thread.sleep(500);
    }
    catch (InterruptedException e) {
      e.printStackTrace();
    }

    String logUri = cluster.getLogUri();
    DateTime executionDateTime = new DateTime(cluster.getStatus().getTimeline().getReadyDateTime());


    String logPathToMapReduceLogs = logUri
        .concat(this.getClusterId())
        .concat("/")
        .concat("hadoop-mapreduce")
        .concat("/")
        .concat("history")
        .concat("/")
        .concat(String.valueOf(executionDateTime.getYear()))
        .concat("/")
        .concat(StringUtils.leftPad(String.valueOf(executionDateTime.getMonthOfYear()), 2, "0"))
        .concat("/")
        .concat(StringUtils.leftPad(String.valueOf(executionDateTime.getDayOfMonth()), 2, "0"))
        .concat("/")
        .concat("000000");

    for (S3ObjectSummary logFile : AwsS3.listLeafItemsAtPrefix(logPathToMapReduceLogs)) {
      if (logFile.getKey().endsWith("jhist.gz")) {
        Long startTime = 0L;
        Long endTime = 0L;
        String jobName = "";
        String jobId = "";
        for (String logLine : AwsS3.getS3ObjectContents(logFile.getBucketName(), logFile.getKey())) {
          org.json.simple.parser.JSONParser jsonParser = new org.json.simple.parser.JSONParser();
          JSONObject logRoot = null;
          try {
            logRoot = ((JSONObject) jsonParser.parse(logLine));
          }
          catch (org.json.simple.parser.ParseException e) {
            log.debug("Parse exception in reading map reduce log file at " + logFile);
            continue;
          }
          if (logRoot.containsKey("type")) {
            String type = logRoot.get("type").toString();
            switch (type) {
              case "JOB_SUBMITTED": {
                startTime = Long.parseLong(((JSONObject) ((JSONObject) logRoot.get("event")).get(
                    "org.apache.hadoop.mapreduce.jobhistory.JobSubmitted")).get("submitTime").toString());
                jobName = ((JSONObject) ((JSONObject) logRoot.get("event")).get(
                    "org.apache.hadoop.mapreduce.jobhistory.JobSubmitted")).get("jobName").toString();
                jobId = ((JSONObject) ((JSONObject) logRoot.get("event")).get(
                    "org.apache.hadoop.mapreduce.jobhistory.JobSubmitted")).get("jobid").toString();
                break;
              }
              case "JOB_FINISHED": {
                endTime = Long.parseLong(((JSONObject) ((JSONObject) logRoot.get("event")).get(
                    "org.apache.hadoop.mapreduce.jobhistory.JobFinished")).get("finishTime").toString());
                break;
              }
              default:
                break;
            }
          }
        }

        emrStepList.add(new EmrStep(jobId, jobName, startTime, endTime, endTime - startTime));
      }
    }

    return emrStepList;
  }

  private long getEmrIdleTime()
  {

    long emrIdleTimeInMillis = 0L;

    long previousStepEndTime = new DateTime(getAmazonElasticMapReduceClient().describeCluster(new DescribeClusterRequest()
                                                                                                  .withClusterId(
                                                                                                      getClusterId()))
                                                                             .getCluster()
                                                                             .getStatus()
                                                                             .getTimeline()
                                                                             .getReadyDateTime()).getMillis();

    final List<MineEMRLogsV2.EmrStep> emrStepList = getSteps();

    for (MineEMRLogsV2.EmrStep stepSummary : emrStepList) {

      Long stepStartTime = stepSummary.startTime;
      Long stepEndTime = stepSummary.endTime;

      emrIdleTimeInMillis += (stepStartTime - previousStepEndTime);

      previousStepEndTime = stepEndTime;


    }

    return emrIdleTimeInMillis;
  }

  private AmazonElasticMapReduce getAmazonElasticMapReduceClient()
  {
    log.info("Getting Amazon EMR Client");

    return AmazonElasticMapReduceClientBuilder
        .standard()
        .withCredentials(new SystemPropertiesCredentialsProvider())
        .build();


  }

  public static void main(String[] args)
  {
    System.out.println("EMR Cluster with ID "
                       + args[0]
                       + " has an idle time of "
                       + new MineEMRLogsV2(args[0]).getEmrIdleTime());
  }

}
