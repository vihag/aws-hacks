import com.amazonaws.auth.SystemPropertiesCredentialsProvider;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduce;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduceClientBuilder;
import com.amazonaws.services.elasticmapreduce.model.DescribeClusterRequest;
import com.amazonaws.services.elasticmapreduce.model.ListStepsRequest;
import com.amazonaws.services.elasticmapreduce.model.StepSummary;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import org.joda.time.DateTime;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Mine EMR logs when the EMR is being used as an "EMR", i.e. the jobs are submitted to the EMR via the aws cli or sdk as "Steps" and not local spark-submits or map-reduces or spark-shells.
 * In this example, we will mine the logs to calculate the total idle time that is observed on the EMR between subsequent step submissions and completions
 */
@Slf4j
@AllArgsConstructor
public class MineEMRLogsV1
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

    log.info("Querying EMR with id " + this.getClusterId() + " for the steps executed on the EMR");

    List<EmrStep> emrStepList = new ArrayList<EmrStep>();

    for (StepSummary stepSummary : getAmazonElasticMapReduceClient().listSteps(new ListStepsRequest().withClusterId(this.getClusterId()))
                                                                    .getSteps()) {


      String mainClass = null;

      for (int i = 0; i < stepSummary.getConfig().getArgs().size(); i++) {
        if (stepSummary.getConfig().getArgs().get(i).equals("--class") || stepSummary.getConfig()
                                                                                     .getArgs()
                                                                                     .get(i)
                                                                                     .equals("-i")) {
          mainClass = stepSummary.getConfig().getArgs().get(i + 1);
          break;
        }
      }

      if (null == mainClass) {
        continue;
      }


      EmrStep emrStep = new EmrStep(
          stepSummary.getId(),
          mainClass,
          stepSummary.getStatus().getTimeline().getStartDateTime().getTime(),
          stepSummary.getStatus().getTimeline().getEndDateTime().getTime(),
          (stepSummary.getStatus().getTimeline().getEndDateTime().getTime() - stepSummary.getStatus()
                                                                                         .getTimeline()
                                                                                         .getCreationDateTime()
                                                                                         .getTime())
      );

      emrStepList.add(emrStep);

      log.info("EMR with id " + this.getClusterId() + " has a step " + emrStep.toString());
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

    final List<EmrStep> emrStepList = getSteps();

    Collections.reverse(emrStepList);

    for (EmrStep stepSummary : emrStepList) {

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
                       + new MineEMRLogsV1(args[0]).getEmrIdleTime());
  }

}
