package com.amirhs.AmazonKCLexample;

import java.nio.charset.Charset;
import java.util.List;

import org.apache.log4j.Logger;

import com.amazonaws.services.kinesis.clientlibrary.exceptions.InvalidStateException;
import com.amazonaws.services.kinesis.clientlibrary.exceptions.KinesisClientLibDependencyException;
import com.amazonaws.services.kinesis.clientlibrary.exceptions.ShutdownException;
import com.amazonaws.services.kinesis.clientlibrary.exceptions.ThrottlingException;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorCheckpointer;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.IRecordProcessor;
import com.amazonaws.services.kinesis.clientlibrary.types.InitializationInput;
import com.amazonaws.services.kinesis.clientlibrary.types.ProcessRecordsInput;
import com.amazonaws.services.kinesis.clientlibrary.types.ShutdownInput;
import com.amazonaws.services.kinesis.model.Record;

public class RecordProcessor implements IRecordProcessor {
	private static Logger log = Logger.getLogger(RecordProcessor.class);
	private String shard = new String();
	public void initialize(InitializationInput shardId) {
		log.info("Initialized RecordProcessor for shard: " + shardId.getShardId());
		this.shard = shardId.getShardId();
		
	}

	public void processRecords(ProcessRecordsInput records) {
		IRecordProcessorCheckpointer checkpointer = records.getCheckpointer();
		List<Record> lRecords = records.getRecords();
		
		for (Record record: lRecords) {
			log.info(record.getApproximateArrivalTimestamp() + "\t" + record.getPartitionKey() + "\t" + record.getSequenceNumber() + "\t" + new String(record.getData().array(), Charset.forName("UTF-8")));
			
		}
	}

	public void shutdown(ShutdownInput arg) {
		log.info("Shutting down RecordProcessor for shard: " + shard + " Reason: " + arg.getShutdownReason());
		checkpoint(arg.getCheckpointer());
	}	
	
	private void checkpoint (IRecordProcessorCheckpointer checkpointer,Record record) {
		try {
			checkpointer.checkpoint(record);
		} catch (KinesisClientLibDependencyException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InvalidStateException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ThrottlingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ShutdownException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	private void checkpoint (IRecordProcessorCheckpointer checkpointer) {
		try {
			checkpointer.checkpoint();
		} catch (KinesisClientLibDependencyException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InvalidStateException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ThrottlingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ShutdownException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
