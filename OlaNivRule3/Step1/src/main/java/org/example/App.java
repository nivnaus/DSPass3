package org.example;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.AmazonEC2ClientBuilder;
import com.amazonaws.services.ec2.model.InstanceType;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduce;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduceClientBuilder;
import com.amazonaws.services.elasticmapreduce.model.*;

import java.util.ArrayList;
import java.util.List;

public class App {
    public static AWSCredentialsProvider credentialsProvider;
    public static AmazonS3 S3;
    public static AmazonEC2 ec2;
    public static AmazonElasticMapReduce emr;

    public static int numberOfInstances = 7;

    public static void main(String[]args){
        credentialsProvider = new ProfileCredentialsProvider();
        System.out.println("[INFO] Connecting to aws");
        ec2 = AmazonEC2ClientBuilder.standard()
                .withCredentials(credentialsProvider)
                .withRegion("us-east-1")
                .build();
        S3 = AmazonS3ClientBuilder.standard()
                .withCredentials(credentialsProvider)
                .withRegion("us-east-1")
                .build();
        emr = AmazonElasticMapReduceClientBuilder.standard()
                .withCredentials(credentialsProvider)
                .withRegion("us-east-1")
                .build();
        System.out.println("list cluster");
        System.out.println(emr.listClusters());

        // Step 1 todo: return

        HadoopJarStepConfig step1 = new HadoopJarStepConfig()
                .withJar("s3://nivolarule05032025/jars/Step1.jar")
                .withMainClass("Step1");

        StepConfig stepConfig1 = new StepConfig()
                .withName("Step1")
                .withHadoopJarStep(step1)
                .withActionOnFailure("TERMINATE_JOB_FLOW");

        // Step 2
        HadoopJarStepConfig step2 = new HadoopJarStepConfig()
                .withJar("s3://nivolarule05032025/jars/Step2.jar")
                .withMainClass("Step2");

        StepConfig stepConfig2 = new StepConfig()
                .withName("Step2")
                .withHadoopJarStep(step2)
                .withActionOnFailure("TERMINATE_JOB_FLOW");

        // Step 3
        HadoopJarStepConfig step3 = new HadoopJarStepConfig()
                .withJar("s3://nivolarule05032025/jars/Step3.jar")
                .withMainClass("Step3");

        StepConfig stepConfig3 = new StepConfig()
                .withName("Step3")
                .withHadoopJarStep(step3)
                .withActionOnFailure("TERMINATE_JOB_FLOW");

        // Step 4
        HadoopJarStepConfig step4 = new HadoopJarStepConfig()
                .withJar("s3://nivolarule05032025/jars/Step4.jar")
                .withMainClass("Step4");

        StepConfig stepConfig4 = new StepConfig()
                .withName("Step4")
                .withHadoopJarStep(step4)
                .withActionOnFailure("TERMINATE_JOB_FLOW");

        // Step 5
        HadoopJarStepConfig step5 = new HadoopJarStepConfig()
                .withJar("s3://nivolarule05032025/jars/Step_5.jar")
                .withMainClass("Step5");

        StepConfig stepConfig5 = new StepConfig()
                .withName("Step5")
                .withHadoopJarStep(step5)
                .withActionOnFailure("TERMINATE_JOB_FLOW");


        //Job flow
        JobFlowInstancesConfig instances = new JobFlowInstancesConfig()
                .withInstanceCount(numberOfInstances)
                .withMasterInstanceType(InstanceType.M4Large.toString())
                .withSlaveInstanceType(InstanceType.M4Large.toString())
                .withHadoopVersion("2.9.2")
                .withEc2KeyName("vockey")
                .withKeepJobFlowAliveWhenNoSteps(false)
                .withPlacement(new PlacementType("us-east-1a"));

        System.out.println("Set steps");
        RunJobFlowRequest runFlowRequest = new RunJobFlowRequest()
                .withName("Map reduce project")
                .withInstances(instances)
//                .withSteps(stepConfig1, stepConfig2, stepConfig3, stepConfig4, stepConfig5)
                .withSteps(stepConfig5)
                .withLogUri("s3://nivolarule05032025/logs/")
                .withServiceRole("EMR_DefaultRole")
                .withJobFlowRole("EMR_EC2_DefaultRole")
                .withReleaseLabel("emr-5.11.0");

        RunJobFlowResult runJobFlowResult = emr.runJobFlow(runFlowRequest);
        String jobFlowId = runJobFlowResult.getJobFlowId();
        System.out.println("Ran job flow with id: " + jobFlowId);
    }
}
