package com.amirhs.AmazonKCLexample;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.UUID;

import org.apache.log4j.Logger;

import com.amazonaws.AmazonClientException;
import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.IRecordProcessorFactory;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.KinesisClientLibConfiguration;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.Worker;

public class Main {
	private static Logger log = Logger.getLogger(Main.class);
	private static AWSCredentialsProvider credentialsProvider;
	private static final String STREAM_NAME = System.getProperty("stream");
	private static final String APPLICATION_NAME = System.getProperty("name");
	private static final String STREAM_POSITION = System.getProperty("position");
	private String workerId; 
	
	private KinesisClientLibConfiguration kinesisClientLibConfiguration;

	private void init() throws UnknownHostException {
        java.security.Security.setProperty("networkaddress.cache.ttl", "0");
        
        workerId = InetAddress.getLocalHost().getCanonicalHostName() + ":" + UUID.randomUUID();

        credentialsProvider = new ProfileCredentialsProvider();
        
        try {
            credentialsProvider.getCredentials();
        } catch (Exception e) {
            throw new AmazonClientException("Cannot load the credentials.\n" + e.getLocalizedMessage());
        }
        
       	if (STREAM_NAME==null) {
       		log.error("Stream name not found in System.properties. Please set \"-Dstream=streamname\"");
       		System.exit(1);
       	}
       	
       	if (APPLICATION_NAME==null) {
       		log.error("Application name not found in System.properties. Please set \"-Dname=appname\"");
       		System.exit(1);
       	}

       	if (STREAM_POSITION==null) {
       		log.error("Strem position (TRIM_HORIZON,LATEST) not found in System.properties. Please set \"-Dposition=position\"");
       		System.exit(1);
       	}

       
        kinesisClientLibConfiguration =
                new KinesisClientLibConfiguration(APPLICATION_NAME,
                        STREAM_NAME,
                        credentialsProvider,
                        workerId);
   
        
        ClientConfiguration config = new ClientConfiguration();
     
        kinesisClientLibConfiguration.withInitialPositionInStream(InitialPositionInStream.valueOf(STREAM_POSITION)).withKinesisClientConfig(config).withRegionName(Region.getRegion(Regions.AP_SOUTHEAST_2).getName()).withIdleTimeBetweenReadsInMillis(60000L).withFailoverTimeMillis(120000L);
        
    }

	public void run() throws UnknownHostException {
		this.init();
		IRecordProcessorFactory recordProcessorFactory = new RecordProcessorFactory();
		Worker worker = new Worker.Builder()
			    .recordProcessorFactory(recordProcessorFactory)
			    .config(this.kinesisClientLibConfiguration)
			    .build();
		
        log.info("Running " + APPLICATION_NAME + " to process stream " + STREAM_NAME + " as worker " + workerId);

        int exitCode = 0;
        try {
            worker.run();
        } catch (Throwable t) {
            System.err.println("Caught throwable while processing data.");
            t.printStackTrace();
            exitCode = 1;
        }
        System.exit(exitCode);
	}
	
	public void Main() {
	}
	public static void main(String argv[]) throws UnknownHostException {
		
		Main main = new Main();
		main.run();
	}
}