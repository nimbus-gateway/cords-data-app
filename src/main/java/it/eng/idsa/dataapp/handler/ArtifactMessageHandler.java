package it.eng.idsa.dataapp.handler;

import static de.fraunhofer.iais.eis.util.Util.asList;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Base64;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.io.OutputStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import de.fraunhofer.iais.eis.ArtifactRequestMessage;
import de.fraunhofer.iais.eis.ArtifactResponseMessageBuilder;
import de.fraunhofer.iais.eis.Message;
import it.eng.idsa.dataapp.service.SelfDescriptionService;
import it.eng.idsa.dataapp.service.ThreadService;
import it.eng.idsa.dataapp.util.BigPayload;
import it.eng.idsa.dataapp.web.rest.exceptions.BadParametersException;
import it.eng.idsa.dataapp.web.rest.exceptions.NotFoundException;
import it.eng.idsa.multipart.util.DateUtil;
import it.eng.idsa.multipart.util.UtilMessageService;


import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import com.fasterxml.jackson.databind.ObjectMapper;



@Component
public class ArtifactMessageHandler extends DataAppMessageHandler {

	private Boolean encodePayload;
	private SelfDescriptionService selfDescriptionService;
	private ThreadService threadService;
	private Path dataLakeDirectory;
	private Boolean contractNegotiationDemo;

	public String arti;
	public Object requestPayload;

	private static final Logger logger = LoggerFactory.getLogger(ArtifactMessageHandler.class);

	public ArtifactMessageHandler(SelfDescriptionService selfDescriptionService, ThreadService threadService,
			@Value("${application.dataLakeDirectory}") Path dataLakeDirectory,
			@Value("${application.contract.negotiation.demo}") Boolean contractNegotiationDemo,
			@Value("#{new Boolean('${application.encodePayload:false}')}") Boolean encodePayload) {
		this.selfDescriptionService = selfDescriptionService;
		this.threadService = threadService;
		this.dataLakeDirectory = dataLakeDirectory;
		this.contractNegotiationDemo = contractNegotiationDemo;
		this.encodePayload = encodePayload;
	}

	@Override
	public Map<String, Object> handleMessage(Message message, Object payload) {
		logger.info("Handling header through ArtifactMessageHandler");

		ArtifactRequestMessage arm = (ArtifactRequestMessage) message;
		Message artifactResponseMessage = null;
		Map<String, Object> response = new HashMap<>();

		logger.info("Payload Recieved:" + payload.toString());
		
		if (arm.getRequestedArtifact() != null) {

			logger.info("Handling message with requestedElement:" + arm.getRequestedArtifact());

			arti = arm.getRequestedArtifact().toString();
			requestPayload = payload;


			logger.info("Prining the ArtifactRequestMessage:" + arm.toString());
	
			if (Boolean.TRUE.equals(((Boolean) threadService.getThreadLocalValue("wss")))) {
				logger.debug("Handling message with requestedElement:" + arm.getRequestedArtifact() + " in WSS flow");
				payload = handleWssFlow(message);
			} else {
				logger.debug("Handling message with requestedElement:" + arm.getRequestedArtifact() + " in REST flow");
				payload = handleRestFlow(message);
			}
		} else {
			logger.error("Artifact requestedElement not provided");

			throw new BadParametersException("Artifact requestedElement not provided", message);
		}

		artifactResponseMessage = createArtifactResponseMessage(arm);
		response.put(DataAppMessageHandler.HEADER, artifactResponseMessage);
		response.put(DataAppMessageHandler.PAYLOAD, payload);

		return response;
	}

	private String handleWssFlow(Message message) {
		String reqArtifact = ((ArtifactRequestMessage) message).getRequestedArtifact().getPath();
		String requestedArtifact = reqArtifact.substring(reqArtifact.lastIndexOf('/') + 1);

		if (contractNegotiationDemo) {
			logger.info("WSS Demo, reading directly from data lake");
			return readFile(requestedArtifact, message);
		} else {
			if (selfDescriptionService.artifactRequestedElementExist((ArtifactRequestMessage) message,
					selfDescriptionService.getSelfDescription(message))) {
				return readFile(requestedArtifact, message);
			} else {
				logger.error("Artifact requestedElement not exist in self description");

				throw new NotFoundException("Artifact requestedElement not found in self description", message);
			}
		}
	}

	private String readFile(String requestedArtifact, Message message) {
		logger.info("Reading file {} from datalake", requestedArtifact);
		byte[] fileContent;
		try {
			fileContent = Files.readAllBytes(dataLakeDirectory.resolve(requestedArtifact));
		} catch (IOException e) {
			logger.error("Could't read the file {} from datalake", requestedArtifact);

			throw new NotFoundException("Could't read the file from datalake", message);

		}
		String base64EncodedFile = Base64.getEncoder().encodeToString(fileContent);
		logger.info("File read from disk.");
		return base64EncodedFile;
	}

	private String handleRestFlow(Message message) {
		String payload = null;
		// Check if requested artifact exist in self description
		if (contractNegotiationDemo || selfDescriptionService.artifactRequestedElementExist(
				(ArtifactRequestMessage) message, selfDescriptionService.getSelfDescription(message))) {
			if (isBigPayload(((ArtifactRequestMessage) message).getRequestedArtifact().toString())) {
				payload = encodePayload == true ? encodePayload(BigPayload.BIG_PAYLOAD.getBytes())
						: BigPayload.BIG_PAYLOAD;
				return payload;
			} else {
				payload = encodePayload == true ? encodePayload(createResponsePayload().getBytes())
						: createResponsePayload();
				return payload;
			}
		} else {
			logger.error("Artifact requestedElement not exist in self description");

			throw new NotFoundException("Artifact requestedElement not found in self description", message);
		}
	}

	private boolean isBigPayload(String path) {
		String isBig = path.substring(path.lastIndexOf('/'));
		if (isBig.equals("/big")) {
			return true;
		}

		return false;
	}

	private String encodePayload(byte[] payload) {
		logger.info("Encoding payload");

		return Base64.getEncoder().encodeToString(payload);
	}


	
	private Map<String, Object> getOpcUaCurrentStatus(){

		try {
            String apiUrl = "http://10.0.2.15:4000/cords/opcua/current";
			
			logger.info("Calling External Service: " + apiUrl);


            URL url = new URL(apiUrl);
            HttpURLConnection connection = (HttpURLConnection) url.openConnection();
            connection.setRequestMethod("GET");

            int responseCode = connection.getResponseCode();

			logger.info("Response Code: " + responseCode);
    

            if (responseCode == HttpURLConnection.HTTP_OK) {
                // Read the response from the API
                BufferedReader reader = new BufferedReader(new InputStreamReader(connection.getInputStream()));
                StringBuilder response = new StringBuilder();
                String line;

                while ((line = reader.readLine()) != null) {
                    response.append(line);
                }
                reader.close();

                // Convert the StringBuilder to a Map using Jackson
				ObjectMapper mapper = new ObjectMapper();
				Object jsonObject = mapper.readValue(response.toString(), Object.class);
				Map<String, Object> responseMap = (Map<String, Object>) jsonObject;

                // Print the Map
                System.out.println("Response Data: " + jsonObject);
				connection.disconnect();
				return responseMap;
            } else {
                System.out.println("GET request failed");
				connection.disconnect();
				return null;
            }

            

        } catch (IOException e) {
            e.printStackTrace();
			return null;
        }
	}


	private Map<String, String> callCordsMlModelManager(String artifactId, Map<String, String> payload) {
		try {
			String apiUrl = "http://10.0.2.15:5000/api/dataspace_resource/initiate_download";
			logger.info("Calling External Service: " + apiUrl);
	
			URL url = new URL(apiUrl);
			HttpURLConnection connection = (HttpURLConnection) url.openConnection();
			connection.setRequestMethod("POST");
	
			// Set headers to indicate that we are sending and expecting JSON
			connection.setRequestProperty("Content-Type", "application/json");
			connection.setRequestProperty("Accept", "application/json");
	
			// Enable sending data to the connection output stream
			connection.setDoOutput(true);
	
			// Convert the payload map to a JSON string
			ObjectMapper mapper = new ObjectMapper();
			String jsonInputString = mapper.writeValueAsString(payload);
	
			// Write the JSON payload to the connection's output stream
			try (OutputStream os = connection.getOutputStream()) {
				byte[] input = jsonInputString.getBytes("utf-8");
				os.write(input, 0, input.length);
			}
	
			int responseCode = connection.getResponseCode();
			logger.info("Response Code: " + responseCode);
	
			if (responseCode == HttpURLConnection.HTTP_OK) {
				// Read the response from the API
				BufferedReader reader = new BufferedReader(new InputStreamReader(connection.getInputStream()));
				StringBuilder response = new StringBuilder();
				String line;
				while ((line = reader.readLine()) != null) {
					response.append(line);
				}
				reader.close();
	
				// Convert the StringBuilder to a Map<String, String> using Jackson
				Map<String, String> responseMap = mapper.readValue(response.toString(), HashMap.class);
	
				// Print the Map
				System.out.println("Response Data: " + responseMap);
				connection.disconnect();
				return responseMap;
			} else {
				System.out.println("POST request failed");
				connection.disconnect();
				return null;
			}
		} catch (IOException e) {
			e.printStackTrace();
			return null;
		}
	}

	private String createResponsePayload() {
		// Put check sum in the payload
		DateFormat dateFormat = new SimpleDateFormat("2023/07/13 12:34:56");
		Date date = new Date();
		String formattedDate = dateFormat.format(date);
		System.out.println("Creating the Payload for Artifact: "+ arti);

		if(arti.contains("opcua")){
			Map<String, Object> jsonObject = getOpcUaCurrentStatus();

			Gson gson = new GsonBuilder().create();


			return gson.toJson(jsonObject);
		}

		else if(arti.contains("vocab")){
			// Map<String, Object> jsonObject = getOpcUaCurrentStatus();

			System.out.println("Reading Data From :/home/nobody/data1/mlVocab.json ");

			String filePath = "/home/nobody/data1/mlVocab.json"; 
			JsonObject jsonObject =  new JsonObject();
        	try (FileReader reader = new FileReader(filePath)) {
				jsonObject = JsonParser.parseReader(reader).getAsJsonObject();
				System.out.println(jsonObject);
        	} catch (IOException e) {
            	e.printStackTrace();
        	}

			return jsonObject.toString();
		}

		else{


			// Gson gson = new Gson();
			// Map<String, String> payloadMap = gson.fromJson(requestPayload.toString(), Map.class);

			// // Access the data using keys
			// String consumerPort = payloadMap.get("consumer_port");
			// String consumerIp = payloadMap.get("consumer_ip");
			

			Map<String, String> payload = new HashMap<>();
			payload.put("artifact_id", arti);
			payload.put("consumer_ip", "127.0.0.1");
			payload.put("consumer_port", "8765");
			
			Map<String, String> resp = callCordsMlModelManager(arti, payload);

			// Map<String, String> jsonObject = new HashMap<>();
			// jsonObject.put("consumer_ip", "127.0.0.1");
			// jsonObject.put("consumer_port", "8765");
			// jsonObject.put("dateOfBirth", formattedDate);
			// jsonObject.put("address", "591  Franklin Street, Pennsylvania");
			// jsonObject.put("checksum", "ABC123 " + formattedDate);
			Gson gson = new GsonBuilder().create();

			return gson.toJson(resp);

		}
		

		

		
	}

	private Message createArtifactResponseMessage(ArtifactRequestMessage header) {
		// Need to set transferCotract from original message, it will be used in policy
		// enforcement
		return new ArtifactResponseMessageBuilder()._issuerConnector_(whoIAmEngRDProvider())
				._issued_(DateUtil.normalizedDateTime())._modelVersion_(UtilMessageService.MODEL_VERSION)
				._transferContract_(header.getTransferContract())._senderAgent_(whoIAmEngRDProvider())
				._recipientConnector_(header != null ? asList(header.getIssuerConnector()) : asList(whoIAm()))
				._correlationMessage_(header != null ? header.getId() : whoIAm())
				._securityToken_(UtilMessageService.getDynamicAttributeToken()).build();
	}
}
