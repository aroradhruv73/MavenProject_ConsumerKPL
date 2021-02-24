package EMAConsumer;


import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.*; 
import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.json.JSONObject;
import com.amazonaws.services.kinesis.producer.Attempt;

import com.amazonaws.services.kinesis.producer.KinesisProducer;
import com.amazonaws.services.kinesis.producer.UserRecordFailedException;
import com.amazonaws.services.kinesis.producer.UserRecordResult;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

import com.refinitiv.ema.access.AckMsg;
import com.refinitiv.ema.access.Data;
import com.refinitiv.ema.access.DataType;
import com.refinitiv.ema.access.DataType.DataTypes;
import com.refinitiv.ema.access.ElementList;
import com.refinitiv.ema.access.EmaFactory;
import com.refinitiv.ema.access.FieldEntry;
import com.refinitiv.ema.access.FieldList;
import com.refinitiv.ema.access.GenericMsg;
import com.refinitiv.ema.access.Map;
import com.refinitiv.ema.access.MapEntry;
import com.refinitiv.ema.access.Msg;
import com.refinitiv.ema.access.OmmArray;
import com.refinitiv.ema.access.OmmConsumer;
import com.refinitiv.ema.access.OmmConsumerClient;
import com.refinitiv.ema.access.OmmConsumerConfig;
import com.refinitiv.ema.access.OmmConsumerEvent;
import com.refinitiv.ema.access.RefreshMsg;
import com.refinitiv.ema.access.ServiceEndpointDiscovery;
import com.refinitiv.ema.access.ServiceEndpointDiscoveryClient;
import com.refinitiv.ema.access.ServiceEndpointDiscoveryEvent;
import com.refinitiv.ema.access.ServiceEndpointDiscoveryOption;
import com.refinitiv.ema.access.ServiceEndpointDiscoveryResp;
import com.refinitiv.ema.access.StatusMsg;
import com.refinitiv.ema.access.UpdateMsg;
import com.refinitiv.ema.rdm.EmaRdm;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.athena.model.QueryExecutionContext;
import software.amazon.awssdk.services.athena.model.ResultConfiguration;
import software.amazon.awssdk.services.athena.model.StartQueryExecutionRequest;
import software.amazon.awssdk.services.athena.model.StartQueryExecutionResponse;
import software.amazon.awssdk.services.athena.model.GetQueryExecutionRequest;
import software.amazon.awssdk.services.athena.model.GetQueryExecutionResponse;
import software.amazon.awssdk.services.athena.model.QueryExecutionState;
import software.amazon.awssdk.services.athena.model.GetQueryResultsRequest;
import software.amazon.awssdk.services.athena.model.GetQueryResultsResponse;
import software.amazon.awssdk.services.athena.AthenaClient;
import software.amazon.awssdk.services.athena.model.AthenaException;
import software.amazon.awssdk.services.athena.model.ColumnInfo;
import software.amazon.awssdk.services.athena.model.Row;
import software.amazon.awssdk.services.athena.model.Datum;
import software.amazon.awssdk.services.athena.paginators.GetQueryResultsIterable;



class AppClient implements OmmConsumerClient, ServiceEndpointDiscoveryClient
{
	private static Logger logger = LogManager.getLogger(AppClient.class);
    
    /**   * Timestamp we'll attach to every record */
    private static final String TIMESTAMP = Long.toString(System.currentTimeMillis());
    public ProducerConfig config = new ProducerConfig();
	
    public KinesisProducer producer = new KinesisProducer(config.transformToKinesisProducerConfiguration());
    
    // The monotonically increasing sequence number we will put in the data of each record
    final AtomicLong sequenceNumber = new AtomicLong(0);
    
    // The number of records that have finished (either successfully put, or failed)
    final AtomicLong completed = new AtomicLong(0);
    
    final ExecutorService callbackThreadPool = Executors.newCachedThreadPool();
    
    public ScheduledExecutorService EXECUTOR = Executors.newScheduledThreadPool(1);

    JSONObject jsonResponse = null;
	public AtomicLong inComingMsgCount = new AtomicLong(0); public AtomicLong outGoingMsgCount = new AtomicLong(0);
	
	public void onRefreshMsg(RefreshMsg refreshMsg, OmmConsumerEvent event)
	{
		String messageType = "RefreshMsg";
		if (DataType.DataTypes.FIELD_LIST == refreshMsg.payload().dataType())
			jsonResponse = decode(refreshMsg.payload().fieldList(),refreshMsg.name(),messageType);
		
		System.out.println(jsonResponse.toString(1));
		logger.debug(jsonResponse.toString(1));
		
		/* KPL way of submitting data  */
		try {
			readRecordsAndSubmitToKPL(jsonResponse);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			
			logger.error("Exception during the record submit in RefreshMsg",e);
		}
		 /*------------------ends here */
	}	
	
	public void onUpdateMsg(UpdateMsg updateMsg, OmmConsumerEvent event) 
	{
		String messageType = "UpdateMsg";
		if (DataType.DataTypes.FIELD_LIST == updateMsg.payload().dataType())
			jsonResponse = decode(updateMsg.payload().fieldList(),updateMsg.name(),messageType);
		
		System.out.println(jsonResponse.toString(1));
		logger.debug(jsonResponse.toString(1));
		
		/* KPL way of submitting data */    
		try {
			readRecordsAndSubmitToKPL(jsonResponse);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			logger.fatal("Exception during the record submit in UpdateMsg",e);
		}
		/*------------------ends here */
	}
	
	public void readRecordsAndSubmitToKPL(JSONObject jsonObject) throws InterruptedException
	{
		//final ScheduledExecutorService EXECUTOR = Executors.newScheduledThreadPool(1);
		final FutureCallback<UserRecordResult> callback = new FutureCallback<UserRecordResult>() {
            @Override
            public void onFailure(Throwable t) {
                // If we see any failures, we will log them.
                int attempts = ((UserRecordFailedException) t).getResult().getAttempts().size()-1;
                if (t instanceof UserRecordFailedException) {
                    Attempt last = ((UserRecordFailedException) t).getResult().getAttempts().get(attempts);
                    if(attempts > 4) {
                        Attempt previous = ((UserRecordFailedException) t).getResult().getAttempts().get(attempts - 1);
                        logger.error(String.format(
                                "Record failed to put - %s : %s. Previous failure - %s : %s",
                                last.getErrorCode(), last.getErrorMessage(), previous.getErrorCode(), previous.getErrorMessage()));
                        //outGoingMsgCount.getAndIncrement();
                    }else{
                        logger.error(String.format(
                                "Record failed to put - %s : %s.",
                                last.getErrorCode(), last.getErrorMessage()));
                        //outGoingMsgCount.getAndIncrement();
                    }

                }
                logger.error("Exception during put", t);
            }

            @Override
            public void onSuccess(UserRecordResult result) {
                //completed.getAndIncrement();
                //logger.info("Sucessfully done");
            	//inComingMsgCount.getAndIncrement();
            	logger.debug("Sucessfully added user record");
            }
        };
        
        // The lines within run() are the essence of the KPL API.
        final Runnable putOneRecord = new Runnable() {
            @Override
            public void run() {
                // TIMESTAMP is our partition key
                try {
                	//double startTime = (System.currentTimeMillis());
	                
                	ListenableFuture<UserRecordResult> f = producer.addUserRecord(config.getStreamName(), TIMESTAMP, Utils.randomExplicitHashKey(), ByteBuffer.wrap(jsonObject.toString().getBytes("UTF-8")));
                	Futures.addCallback(f, callback, callbackThreadPool);
                	//outGoingMsgCount.getAndIncrement();
                	//double endTime = (System.currentTimeMillis() - startTime);
                	//logger.info("Time taken by addUserRecord " + endTime);
				} catch (UnsupportedEncodingException e) {
					// TODO Auto-generated catch block
					logger.fatal("Exception during the record submit",e);
				}
            }
        };
        EXECUTOR.schedule(putOneRecord,400,TimeUnit.MILLISECONDS);
	}
	
	
	public void onStatusMsg(StatusMsg statusMsg, OmmConsumerEvent event) 
	{
		logger.debug(statusMsg);
	}

	public void onGenericMsg(GenericMsg genericMsg, OmmConsumerEvent consumerEvent){}
	public void onAckMsg(AckMsg ackMsg, OmmConsumerEvent consumerEvent){}
	public void onAllMsg(Msg msg, OmmConsumerEvent consumerEvent){}
	
	public JSONObject decode(FieldList fieldList,String name,String msgType)
	{
		JSONObject mainObj = new JSONObject();
		mainObj.put("MessageType", msgType);
		
		if(name.contains("RRPS")) {
			mainObj.put("RICName",name.substring(0,10));
		}
		else {
			mainObj.put("RICName",name);
		}
		
//		mainObj.put("RICName",name);
		String value = null;
		JSONObject subObj = new JSONObject();
		
		for (FieldEntry fieldEntry : fieldList)
		{
			if (Data.DataCode.BLANK == fieldEntry.code())
			    value = " blank";
			else
				switch (fieldEntry.loadType())
				{
				case DataTypes.REAL :
					value = fieldEntry.real().toString(); 
					break;
				case DataTypes.DATE :
					value = fieldEntry.date().day() + " / " + fieldEntry.date().month() + " / " + fieldEntry.date().year();
					break;
				case DataTypes.TIME :
					value = fieldEntry.time().hour() + ":" + fieldEntry.time().minute() + ":" + fieldEntry.time().second() + ":" + fieldEntry.time().millisecond();
					break;
				case DataTypes.DATETIME :
					value = fieldEntry.dateTime().day() + " / " + fieldEntry.dateTime().month() + " / " +
							fieldEntry.dateTime().year() + "." + fieldEntry.dateTime().hour() + ":" + 
							fieldEntry.dateTime().minute() + ":" + fieldEntry.dateTime().second() + ":" + 
							fieldEntry.dateTime().millisecond() + ":" + fieldEntry.dateTime().microsecond()+ ":" + 
							fieldEntry.dateTime().nanosecond();
					break;
				case DataTypes.INT :
					value = fieldEntry.toString();
					break;
				case DataTypes.UINT :
					value = fieldEntry.toString();
					break;
				case DataTypes.ASCII :
					value = fieldEntry.ascii().toString();
					break;
				case DataTypes.ENUM :
					value = fieldEntry.hasEnumDisplay() ? Integer.toString(fieldEntry.enumValue()) : fieldEntry.enumDisplay().toString();
					break;
				case DataTypes.RMTES :
					value = fieldEntry.rmtes().toString();
					break;
				case DataTypes.ERROR :
					value = fieldEntry.error().errorCodeAsString();
					break;
				default :
					System.out.println();
					break;
				}
			subObj.put(fieldEntry.name(), value);
		}
		mainObj.put("Fields", subObj);
		return mainObj;
	}

	public void onSuccess(ServiceEndpointDiscoveryResp serviceEndpointResp, ServiceEndpointDiscoveryEvent event)
	{
		System.out.println(serviceEndpointResp); // dump service discovery endpoints
		
		for(int index = 0; index < serviceEndpointResp.serviceEndpointInfoList().size(); index++)
		{
			List<String> locationList = serviceEndpointResp.serviceEndpointInfoList().get(index).locationList();
			
			if(locationList.size() == 2) // Get an endpoint that provides auto failover for the specified location.
			{
				if(locationList.get(0).startsWith(Consumer.location))
				{
					Consumer.host = serviceEndpointResp.serviceEndpointInfoList().get(index).endpoint();
					Consumer.port = serviceEndpointResp.serviceEndpointInfoList().get(index).port();
					break;
				}
			}
		}
	}

	public void onError(String errorText, ServiceEndpointDiscoveryEvent event)
	{
		logger.error("Failed to query EDP-RT service discovery. Error text: " + errorText);
	}
}

public class Consumer
{
	static String userName;
	static String password;
	static String clientId;
	static String proxyHostName;
	static String proxyPort = "-1";
	static String proxyUserName;
	static String proxyPassword;
	static String proxyDomain;
	static String proxyKrb5Configfile;
	public static String host;
	public static String port;
	public static String location = "us-east";
	
	public static Properties properties;
	
	private static Logger logger = LogManager.getLogger(Consumer.class);
	
	static boolean readCommandlineArgs(String[] args, OmmConsumerConfig config)
	{
	    try
	    {
	        Consumer cons = new Consumer();
	        
	        ClassLoader classLoader = cons.getClass().getClassLoader();
            InputStream fs = classLoader.getResourceAsStream("default_config.properties");
            BufferedReader reader = new BufferedReader(new InputStreamReader(fs));
            
	        Properties properties = new Properties();
	        properties.load(reader);
	        
	        userName = properties.getProperty("userName");
	        password = properties.getProperty("password");
	        clientId = properties.getProperty("clientId");
	        System.out.println("keystore file value  " + properties.getProperty("keyfile"));
	        config.tunnelingKeyStoreFile(properties.getProperty("keyfile"));
	        
	        logger.info("keyfile path is: " + properties.getProperty("keyfile"));
	        
			config.tunnelingSecurityProtocol("TLS");
	        config.tunnelingKeyStorePasswd(properties.getProperty("keypasswd"));
	        proxyHostName = null;
	        proxyPort = null;
	        proxyUserName = null;
	        proxyPassword = null;
	        proxyDomain = null;
	        proxyKrb5Configfile = null;
	        
	        if ( userName == null || password == null || clientId == null)
			{
	        	logger.error("Username, password, and clientId must be specified on the command line. Exiting...");
				return false;
			}
     }
     catch (Exception e)
     {
     	logger.error("Reading authentication key values from configuration",e);
         return false;
     }
		return true;
	}
	
	static void createProgramaticConfig(Map configDb)
	{
		try {
		
			Map elementMap = EmaFactory.createMap();
			ElementList elementList = EmaFactory.createElementList();
			ElementList innerElementList = EmaFactory.createElementList();
			
			innerElementList.add(EmaFactory.createElementEntry().ascii("Channel", "Channel_1"));
			
			elementMap.add(EmaFactory.createMapEntry().keyAscii("Consumer_1", MapEntry.MapAction.ADD, innerElementList));
			innerElementList.clear();
			
			elementList.add(EmaFactory.createElementEntry().map("ConsumerList", elementMap));
			elementMap.clear();
			
			configDb.add(EmaFactory.createMapEntry().keyAscii("ConsumerGroup", MapEntry.MapAction.ADD, elementList));
			elementList.clear();
			
			innerElementList.add(EmaFactory.createElementEntry().ascii("ChannelType", "ChannelType::RSSL_ENCRYPTED"));
//			innerElementList.add(EmaFactory.createElementEntry().ascii("Host", host));
//			innerElementList.add(EmaFactory.createElementEntry().ascii("Port", port));
//			innerElementList.add(EmaFactory.createElementEntry().ascii("Host", "amer-3.pricing.streaming.edp.thomsonreuters.com"));
			innerElementList.add(EmaFactory.createElementEntry().ascii("Host", "amer-3-t2.streaming-pricing-api.refinitiv.com"));
			innerElementList.add(EmaFactory.createElementEntry().ascii("Port", "14002"));
			innerElementList.add(EmaFactory.createElementEntry().intValue("EnableSessionManagement", 1));
			
			elementMap.add(EmaFactory.createMapEntry().keyAscii("Channel_1", MapEntry.MapAction.ADD, innerElementList));
			innerElementList.clear();
			
			elementList.add(EmaFactory.createElementEntry().map("ChannelList", elementMap));
			elementMap.clear();
			
			configDb.add(EmaFactory.createMapEntry().keyAscii("ChannelGroup", MapEntry.MapAction.ADD, elementList));
		}
		catch(Exception e) {
			logger.error("Failed in creating channels");
		}
	}
	
	
	public static void main(String[] args) throws IOException
	{
		System.out.println("Main Program starting");
		
		OmmConsumer consumer = null;
		ServiceEndpointDiscovery serviceDiscovery = null;
		AppClient appClient = new AppClient();
		try
		{
			serviceDiscovery = EmaFactory.createServiceEndpointDiscovery();
			OmmConsumerConfig config = EmaFactory.createOmmConsumerConfig();
			Map configDb = EmaFactory.createMap();
			
			System.out.println("Before Reading From Properties File");
			if (!readCommandlineArgs(args, config)) return;
			System.out.println("After Reading From Properties File");
			serviceDiscovery.registerClient(EmaFactory.createServiceEndpointDiscoveryOption().username(userName)
					.password(password).clientId(clientId).transport(ServiceEndpointDiscoveryOption.TransportProtocol.TCP)
					.proxyHostName(proxyHostName).proxyPort(proxyPort).proxyUserName(proxyUserName)
					.proxyPassword(proxyPassword).proxyDomain(proxyDomain).proxyKRB5ConfigFile(proxyKrb5Configfile), appClient); 
			
			if ( host == null || port == null )
			{
				logger.error("Both hostname and port are not avaiable for establishing a connection with ERT in cloud. Exiting...");
				return;
			}
			
			createProgramaticConfig(configDb);
			
			if ( (proxyHostName == null) && (proxyPort == "-1") )
			{
				consumer  = EmaFactory.createOmmConsumer(config.consumerName("Consumer_1").username(userName).password(password)
					.clientId(clientId).config(configDb));
			}
			else
			{
				consumer  = EmaFactory.createOmmConsumer(config.consumerName("Consumer_1").username(userName).password(password)
					.clientId(clientId).config(configDb).tunnelingProxyHostName(proxyHostName).tunnelingProxyPort(proxyPort)
					.tunnelingCredentialUserName(proxyUserName).tunnelingCredentialPasswd(proxyPassword).tunnelingCredentialDomain(proxyDomain)
					.tunnelingCredentialKRB5ConfigFile(proxyKrb5Configfile)); 
			}
			
			//Consumer cons = new Consumer();
			String line = null;
			
//			InputStream is = Consumer.class.getResourceAsStream("../RIC.csv");
//			InputStreamReader streamReader = new InputStreamReader(is, StandardCharsets.UTF_8);
//			BufferedReader br = new BufferedReader(streamReader);
			
			BufferedReader br = new BufferedReader(new FileReader("//home//ec2-user//RIC2.csv"));
			//BufferedReader br = new BufferedReader(new FileReader("C:\\RIC2.csv"));
			
//			BufferedReader br = new BufferedReader(new FileReader("//home//ubuntu//RIC.csv"));
			
			
//			c:\\KeyStore\\keystore.jks
			
			//Prepare multiple field IDs in an OmmArray
			OmmArray array = EmaFactory.createOmmArray();
			array.fixedWidth(2);
			array.add(EmaFactory.createOmmArrayEntry().intValue(363));//FID363 for ASK_YIELD
			array.add(EmaFactory.createOmmArrayEntry().intValue(362));//FID362 for BID_YIELD
			array.add(EmaFactory.createOmmArrayEntry().intValue(275));//FID275 for ASK
			array.add(EmaFactory.createOmmArrayEntry().intValue(393));//FID393 for BID
			array.add(EmaFactory.createOmmArrayEntry().intValue(4));//FID4 for RDN_EXCHID
			array.add(EmaFactory.createOmmArrayEntry().intValue(365));//FID365 for HIGH_YLD
			array.add(EmaFactory.createOmmArrayEntry().intValue(366));//FID366 for LOW_YLD
			array.add(EmaFactory.createOmmArrayEntry().intValue(875));//FID365 for VALUE_DT1
			array.add(EmaFactory.createOmmArrayEntry().intValue(1010));//FID365 for VALUE_TS1
			array.add(EmaFactory.createOmmArrayEntry().intValue(178));//FID178 for TRDVOL_1
			array.add(EmaFactory.createOmmArrayEntry().intValue(32));//FID32 for ACVOL_1
			array.add(EmaFactory.createOmmArrayEntry().intValue(31));//FID31 for ASK_SIZE
			array.add(EmaFactory.createOmmArrayEntry().intValue(30));//FID30 for BID_SIZE
			
			//Prepare multiple RICs in an OmmArray
			OmmArray arrayI = EmaFactory.createOmmArray();
			OmmArray arrayK = EmaFactory.createOmmArray();
			
			System.out.println("Reading csv");
			int i = 0;
			//Code commented temporarily to experiment with fetching data from Athena table and feed into API 
			while ((line = br.readLine()) != null) {
			      String[] values = line.split(",");
			      for (String str : values) {
	          		   //System.out.println(str);
			    	  
			    	   if(i >= 5000)
			    		   arrayK.add(EmaFactory.createOmmArrayEntry().ascii(str));
			    	   else
			    		   arrayI.add(EmaFactory.createOmmArrayEntry().ascii(str));
			    	   i++;
//			    	   if(i == 5000) break;
			      }
			    }
			br.close(); 
			System.out.println("Number of RICs " + i);
			
			
			//Fetching RICs from Athena table
//			AthenaClient athenaClient = AthenaClient.builder()
//	                .region(Region.US_EAST_1)
//	                .build();
//			String queryExecutionId = submitAthenaQuery(athenaClient);
//	        
//	        try {
//	        	waitForQueryToComplete(athenaClient, queryExecutionId);
//	        	logger.info("end of waitForQueryToComplete");
//	        }
//	        catch(InterruptedException e) {
//	        	logger.error("Failed to complete athena query while starting", e);
//	        }

			//Combine both Batch and View and add them to ElementList
			ElementList batchView = EmaFactory.createElementList();
			//for getting RICsfrom csv file
			batchView.add(EmaFactory.createElementEntry().array(EmaRdm.ENAME_BATCH_ITEM_LIST, arrayI));
			batchView.add(EmaFactory.createElementEntry().uintValue(EmaRdm.ENAME_VIEW_TYPE, 1));
			batchView.add(EmaFactory.createElementEntry().array(EmaRdm.ENAME_VIEW_DATA, array));
			
			//Code commented for athena, the below line after commented one workd with RICs from csv file 
//			OmmArray arrayJ = processResultRows(athenaClient, queryExecutionId,arrayI,batchView,appClient,consumer,array);
			consumer.registerClient(EmaFactory.createReqMsg().serviceName("ELEKTRON_DD").payload(batchView), appClient);
			
			ElementList batchView1 = EmaFactory.createElementList();
			//for getting RICsfrom csv file
			batchView1.add(EmaFactory.createElementEntry().array(EmaRdm.ENAME_BATCH_ITEM_LIST, arrayK));
			batchView1.add(EmaFactory.createElementEntry().uintValue(EmaRdm.ENAME_VIEW_TYPE, 1));
			batchView1.add(EmaFactory.createElementEntry().array(EmaRdm.ENAME_VIEW_DATA, array));
			
			//Code commented for athena, the below line after commented one workd with RICs from csv file 
//			OmmArray arrayJ = processResultRows(athenaClient, queryExecutionId,arrayI,batchView,appClient,consumer,array);
			consumer.registerClient(EmaFactory.createReqMsg().serviceName("ELEKTRON_DD").payload(batchView1), appClient);
			
			logger.info("Registered the Omm Client");
			
			while(true)
			{   
//				Calendar now = Calendar.getInstance();
//				int hour = now.get(Calendar.HOUR_OF_DAY);
//				int minute = now.get(Calendar.MINUTE);
//				int second = now.get(Calendar.SECOND);
//				
//				if(hour == 22 && minute == 30 && second == 0)
//				{
//					///------------------------------------------------------------------------------------------////
//					consumer.uninitialize();
//					serviceDiscovery.uninitialize();
//					
//					appClient.EXECUTOR.awaitTermination(appClient.config.getSecondsToRun() + 1, TimeUnit.SECONDS);
//					appClient.producer.flushSync();
//					
//					appClient=null;
//					appClient = new AppClient();
//					
//					serviceDiscovery = EmaFactory.createServiceEndpointDiscovery();
//					config = EmaFactory.createOmmConsumerConfig();
//					configDb = EmaFactory.createMap();
//					
//					if (!readCommandlineArgs(args, config)) return;
//					serviceDiscovery.registerClient(EmaFactory.createServiceEndpointDiscoveryOption().username(userName)
//							.password(password).clientId(clientId).transport(ServiceEndpointDiscoveryOption.TransportProtocol.TCP)
//							.proxyHostName(proxyHostName).proxyPort(proxyPort).proxyUserName(proxyUserName)
//							.proxyPassword(proxyPassword).proxyDomain(proxyDomain).proxyKRB5ConfigFile(proxyKrb5Configfile), appClient); 
//					
//					if ( host == null || port == null )
//					{
//						logger.error("Both hostname and port are not avaiable for establishing a connection with ERT in cloud. Exiting...");
//						return;
//					}
//					
//					createProgramaticConfig(configDb);
//					
//					if ( (proxyHostName == null) && (proxyPort == "-1") )
//					{
//						consumer  = EmaFactory.createOmmConsumer(config.consumerName("Consumer_1").username(userName).password(password)
//							.clientId(clientId).config(configDb));
//					}
//					else
//					{
//						consumer  = EmaFactory.createOmmConsumer(config.consumerName("Consumer_1").username(userName).password(password)
//							.clientId(clientId).config(configDb).tunnelingProxyHostName(proxyHostName).tunnelingProxyPort(proxyPort)
//							.tunnelingCredentialUserName(proxyUserName).tunnelingCredentialPasswd(proxyPassword).tunnelingCredentialDomain(proxyDomain)
//							.tunnelingCredentialKRB5ConfigFile(proxyKrb5Configfile)); 
//					}
//					///------------------------------------------------------------------------------------------////
//					
//					athenaClient = null;
//					athenaClient = AthenaClient.builder()
//		                .region(Region.US_EAST_1)
//		                .build();
//					queryExecutionId = "";
//					queryExecutionId = submitAthenaQuery(athenaClient);
//        
//			        try {
//			        	waitForQueryToComplete(athenaClient, queryExecutionId);
//			        }
//			        catch(InterruptedException e) {}
//
//			        arrayI = null;
//			        arrayI = EmaFactory.createOmmArray();
//			        arrayJ = null;
//			        
//			        logger.info("Reloading reloading OmmConsumer because a new business day has elapsed");
//			        arrayJ = processResultRows(athenaClient, queryExecutionId,arrayI,batchView,appClient,consumer,array);
//			        logger.info("Finished reloading OmmConsumer.");
//				} 
							
				Thread.sleep(1000); // API calls onRefreshMsg(), onUpdateMsg() and onStatusMsg()
			} 
		} 
		catch (InterruptedException excp)
		{
			logger.fatal("Fatal exception in Main thread",excp);
		}
		finally 
		{
			if (consumer != null) consumer.uninitialize();
			if (serviceDiscovery != null) serviceDiscovery.uninitialize();

			
			// Wait for puts to finish. After this statement returns, we have
	        // finished all calls to putRecord, but the records may still be
	        // in-flight. We will additionally wait for all records to actually
	        // finish later.
			try {
				appClient.EXECUTOR.awaitTermination(appClient.config.getSecondsToRun() + 1, TimeUnit.SECONDS);
			} catch (InterruptedException e) {
				e.printStackTrace();
				logger.error("Error during termination",e);
			}
	        
	        // If you need to shutdown your application, call flushSync() first to
	        // send any buffered records. This method will block until all records
	        // have finished (either success or fail). There are also asynchronous
	        // flush methods available.
	        //
	        // Records are also automatically flushed by the KPL after a while based
	        // on the time limit set with Configuration.setRecordMaxBufferedTime()
	        //log.info("Waiting for remaining puts to finish...");
	        appClient.producer.flushSync();
	        appClient.producer.destroy();
			
			//logger.info("Incoming messages = " + appClient.inComingMsgCount.get());
			//logger.info("OutGoing messages = " + appClient.outGoingMsgCount.get());
		}
	}

	public static String submitAthenaQuery(AthenaClient athenaClient) {

        try {

            // The QueryExecutionContext allows us to set the Database.
            QueryExecutionContext queryExecutionContext = QueryExecutionContext.builder()
                //.database("test").build();
                .database("batch_rail").build();  //batch_rail   // batch-rail [Prod]

             // The result configuration specifies where the results of the query should go in S3 and encryption options
            ResultConfiguration resultConfiguration = ResultConfiguration.builder()
                // You can provide encryption options for the output that is written.
                // .withEncryptionConfiguration(encryptionConfiguration)
                .outputLocation("s3://athenatestdhruv").build();  //     // athenamarketprice [Prod]

            // Create the StartQueryExecutionRequest to send to Athena which will start the query.
            StartQueryExecutionRequest startQueryExecutionRequest = StartQueryExecutionRequest.builder()
//            	.queryString("select distinct col1 from input_list_rics where col1 <> 'ric'")
            	.queryString("select distinct col1 from input_list_rics where col1 <> 'ric'")  //input_list_rics_ce5ec5a53772382fe32ee94132815bf0 [Prod]
                
//            	.queryString("SELECT col1 FROM input_list_rics where col1 IN('DE114179=','DE114179=','DE114178=','DE114178=','DE114177=','DE114177=','DE114176=','DE114176=','DE114175=','DE114175=','DE114174=','DE114174=','DE114173=','DE114173=','DE113549=','DE113549=','DE113548=','DE113548=','DE113547=','DE113547=','DE113546=','DE113546=','DE113545=','DE113545=','DE113544=','DE113544=','DE113543=','DE113543=','DE113542=','DE113542=','DE113536=','DE113536=','DE113532=','DE113532=','DE113527=','DE113527=','DE113522=','DE113522=','DE113517=','DE113517=','DE113514=','DE113514=','DE113508=','DE113508=','DE113506=','DE113506=','DE113504=','DE113504=','DE113492=','DE113492=','DE110476=','DE110476=','DE110475=','DE110475=','DE110474=','DE110474=','DE110247=','DE110247=','DE110246=','DE110246=','DE110245=','DE110245=','DE110244=','DE110244=','DE110243=','DE110243=','DE110242=','DE110242=','DE110241=','DE110241=','DE110240=','DE110240=','DE110239=','DE110239=','DE110238=','DE110238=','DE110237=','DE110237=','DE110236=','DE110236=','DE110235=','DE110235=','DE110234=','DE110234=','DE110233=','DE110233=','DE110232=','DE110232=','DE110231=','DE110231=','DE110230=','DE110230=','DE103057=','DE103057=','DE103056=','DE103056=','DE103055=','DE103055=','DE103054=','DE103054=')")            	
            	
            	.queryExecutionContext(queryExecutionContext)
                .resultConfiguration(resultConfiguration).build();

            StartQueryExecutionResponse startQueryExecutionResponse = athenaClient.startQueryExecution(startQueryExecutionRequest);
            return startQueryExecutionResponse.queryExecutionId();

        } catch (AthenaException e) {
            logger.error("Error during the submission of Athena query",e);
            System.exit(1);
        }
        return "";
    }

    /**
     * Wait for an Athena query to complete, fail or to be cancelled. This is done by polling Athena over an
     * interval of time. If a query fails or is cancelled, then it will throw an exception.
     */

    public static void waitForQueryToComplete(AthenaClient athenaClient, String queryExecutionId) throws InterruptedException {
        GetQueryExecutionRequest getQueryExecutionRequest = GetQueryExecutionRequest.builder()
                .queryExecutionId(queryExecutionId).build();

        GetQueryExecutionResponse getQueryExecutionResponse;
        boolean isQueryStillRunning = true;
        while (isQueryStillRunning) {
            getQueryExecutionResponse = athenaClient.getQueryExecution(getQueryExecutionRequest);
            String queryState = getQueryExecutionResponse.queryExecution().status().state().toString();
            if (queryState.equals(QueryExecutionState.FAILED.toString())) {
                throw new RuntimeException("Query Failed to run with Error Message: " + getQueryExecutionResponse
                        .queryExecution().status().stateChangeReason());
            } else if (queryState.equals(QueryExecutionState.CANCELLED.toString())) {
                throw new RuntimeException("Query was cancelled.");
            } else if (queryState.equals(QueryExecutionState.SUCCEEDED.toString())) {
                isQueryStillRunning = false;
            } else {
                // Sleep an amount of time before retrying again.
                Thread.sleep(1000);
            }
            logger.info("Current Status is: " + queryState);
        }
    }

    /**
     * This code calls Athena and retrieves the results of a query.
     * The query must be in a completed state before the results can be retrieved and
     * paginated. The first row of results are the column headers.
     * @throws IOException 
     */
    public static OmmArray processResultRows(AthenaClient athenaClient, String queryExecutionId,OmmArray arrayI,ElementList batchView,AppClient appClient,OmmConsumer consumer,OmmArray array) throws IOException {

       try {
    	    BufferedWriter writer = new BufferedWriter(new FileWriter("C:\\Users\\Operations\\Desktop\\RICS\\RicsGotIn.csv"));
            GetQueryResultsRequest getQueryResultsRequest = GetQueryResultsRequest.builder()
                // Max Results can be set but if its not set,
                // it will choose the maximum page size
                // As of the writing of this code, the maximum value is 1000
                // .withMaxResults(1000)
                .queryExecutionId(queryExecutionId).build();

            GetQueryResultsIterable getQueryResultsResults = athenaClient.getQueryResultsPaginator(getQueryResultsRequest);
            int j = 0;
            for (GetQueryResultsResponse result : getQueryResultsResults) {
                List<ColumnInfo> columnInfoList = result.resultSet().resultSetMetadata().columnInfo();
                List<Row> results = result.resultSet().rows();
                processRow(results, columnInfoList,arrayI,writer);
                
                batchView.add(EmaFactory.createElementEntry().array(EmaRdm.ENAME_BATCH_ITEM_LIST, arrayI));
    			batchView.add(EmaFactory.createElementEntry().uintValue(EmaRdm.ENAME_VIEW_TYPE, 1));
    			batchView.add(EmaFactory.createElementEntry().array(EmaRdm.ENAME_VIEW_DATA, array));
                try {
                	consumer.registerClient(EmaFactory.createReqMsg().serviceName("ELEKTRON_DD").payload(batchView), appClient);
                }catch(Exception e) {
                	consumer.registerClient(EmaFactory.createReqMsg().serviceName("ELEKTRON_DD").payload(batchView), appClient);
                }
                j = j + 1;
                //consumer.uninitialize();
                arrayI = null;
                arrayI = EmaFactory.createOmmArray();
            }
            
            System.out.println("The value of j is:" + j);

        } catch (AthenaException e) {
           logger.error("Error during process result rows of athena",e);
           System.exit(1);
       }
       return arrayI;
    }

    private static OmmArray processRow(List<Row> row, List<ColumnInfo> columnInfoList,OmmArray arrayI,BufferedWriter writer) throws IOException {

        //Write out the data
        for (Row myRow : row) {
            List<Datum> allData = myRow.data();
            for (Datum data : allData) {
                System.out.println("The value of the column is "+data.varCharValue());
                writer.write(data.varCharValue());
                try {
	                //if(data.varCharValue() == "col1")
	        		//{}
	        		//else {
	        			arrayI.add(EmaFactory.createOmmArrayEntry().ascii(data.varCharValue()));
	        		//}
                }catch(Exception e) {
                		logger.error("Error during the processRow method",e);
                }
            }
        }
        return arrayI;
    }

}
