package org.wso2;

import org.apache.commons.collections.map.HashedMap;
import org.apache.log4j.Logger;
import org.wso2.siddhi.core.SiddhiAppRuntime;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.query.api.SiddhiApp;
import org.wso2.siddhi.query.api.definition.AbstractDefinition;
import org.wso2.siddhi.query.api.exception.SiddhiAppValidationException;
import org.wso2.siddhi.query.api.execution.ExecutionElement;
import org.wso2.siddhi.query.api.execution.partition.Partition;
import org.wso2.siddhi.query.api.execution.partition.PartitionType;
import org.wso2.siddhi.query.api.execution.query.Query;
import org.wso2.siddhi.query.api.execution.query.input.handler.StreamHandler;
import org.wso2.siddhi.query.api.execution.query.input.handler.Window;
import org.wso2.siddhi.query.api.execution.query.input.stream.InputStream;
import org.wso2.siddhi.query.api.execution.query.input.stream.JoinInputStream;
import org.wso2.siddhi.query.api.execution.query.input.stream.SingleInputStream;
import org.wso2.siddhi.query.api.execution.query.input.stream.StateInputStream;
import org.wso2.siddhi.query.api.util.ExceptionUtil;
import org.wso2.siddhi.query.compiler.SiddhiCompiler;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class SiddhiAppSplitter {

    private static final Logger logger = Logger.getLogger(SiddhiAppSplitter.class);
    private Map<String, StrSiddhiApp> distributiveMap;
    private Map<String, String> inmemoryMap;
    private SiddhiAppRuntime siddhiAppRuntime;
    private SiddhiApp siddhiApp;
    private String appName;
    private String siddhiAppString;
    private Map<String, PartitionType> partitionTypeMap;
    private int[] queryContextEndIndex;
    private int[] queryContextStartIndex;

    public SiddhiAppSplitter() {
        this.distributiveMap = new LinkedHashMap<String, StrSiddhiApp>();
        partitionTypeMap = new HashMap<String, PartitionType>();
        this.inmemoryMap = new HashedMap();


    }

    public void distributeSiddiApp(String siddhiAppString) {

        this.siddhiAppString = siddhiAppString;
        this.siddhiApp = SiddhiCompiler.parse(siddhiAppString);
        this.siddhiAppRuntime = (new SiddhiManager()).createSiddhiAppRuntime(siddhiAppString);

        StrSiddhiApp siddhiAppdist;
        String groupName;
        int parallel;
        boolean queryElement;


        //getSiddhiAppName
        appName = getAppName();

        for (ExecutionElement executionElement : siddhiApp.getExecutionElementList()) {

            parallel = 1;
            groupName = null;

            for (int i = 0; i < executionElement.getAnnotations().size(); i++) {
                if (executionElement.getAnnotations().get(i).getElement("execGroup") != null) {
                    groupName = appName + "-" + executionElement.getAnnotations().get(i).getElement("execGroup");

                }

                if (executionElement.getAnnotations().get(i).getElement("parallel") != null) {
                    parallel = Integer.parseInt(executionElement.getAnnotations().get(i).getElement("parallel"));

                }
            }
            if (groupName != null && !distributiveMap.containsKey(groupName)) {

                siddhiAppdist = new StrSiddhiApp();
                siddhiAppdist.setAppName(groupName);
                siddhiAppdist.setParallel(Integer.toString(parallel));

            } else if (distributiveMap.containsKey(groupName)) {
                siddhiAppdist = distributiveMap.get(groupName);

                //Same execution group given  with different parallel numbers
                if (!siddhiAppdist.getParallel().equals(Integer.toString(parallel))) {

                    throw new SiddhiAppValidationException("execGroup =" + "\'" + groupName + "\' not assigned a unique @dist(parallel)");
                }
            } else

            {
                //will work if execGroup is not mentioned-those will go to a single app
                siddhiAppdist = new StrSiddhiApp();
                siddhiAppdist.setAppName(appName + "-000");
                siddhiAppdist.setParallel(Integer.toString(parallel));
            }

            //if execution element is a query
            if (executionElement instanceof Query) {

                //set query

                queryContextStartIndex = ((Query) executionElement).getQueryContextStartIndex();
                queryContextEndIndex = ((Query) executionElement).getQueryContextEndIndex();
                siddhiAppdist.setQuery(removeMetainfo((Query) executionElement, ExceptionUtil.getContext(queryContextStartIndex, queryContextEndIndex, siddhiAppString)));

                queryElement = true;
                distributiveMap.put(groupName, setStream((Query) executionElement, groupName, siddhiAppdist, queryElement));


            } else if (executionElement instanceof Partition) {


                //set query
                queryContextStartIndex = ((Partition) executionElement).getQueryContextStartIndex();
                queryContextEndIndex = ((Partition) executionElement).getQueryContextEndIndex();
                siddhiAppdist.setQuery(removeMetainfo((Partition) executionElement, ExceptionUtil.getContext(queryContextStartIndex, queryContextEndIndex, siddhiAppString)));

                List<Query> partitionQueryList = ((Partition) executionElement).getQueryList();
                for (Query query : partitionQueryList) {
                    for (int k = 0; k < query.getAnnotations().size(); k++) {
                        if (query.getAnnotations().get(k).getElement("dist") != null) {
                            throw new SiddhiAppValidationException("Unsupported:@dist annotation in inside partition queries");
                        }
                    }


                    queryElement = false;
                    partitionTypeMap = ((Partition) executionElement).getPartitionTypeMap();
                    distributiveMap.put(groupName, setStream(query, groupName, siddhiAppdist, queryElement));
                }


            }


        }

        createJsonConfigurationFile();

    }


    private String getAppName() {
        for (int i = 0; i < siddhiApp.getAnnotations().size(); i++) {
            if (siddhiApp.getAnnotations().get(i).getName().equals("name")) {
                return siddhiApp.getAnnotations().get(0).getElements().get(0).getValue();

            }

        }

        return "SiddhiApp";//defaultName
    }


    private String isUserGivenStream(String streamDefinition) {

        if (streamDefinition.toLowerCase().contains("source") || streamDefinition.toLowerCase().contains("sink")) {
            return "true";
        } else {
            return "false";
        }
    }


    private StrSiddhiApp setStream(Query executionElement, String groupName, StrSiddhiApp siddhiAppdist, boolean queryElement) {
        String[] inputStreamDefinition;
        String[] outputStreamDefinition;
        List<String> listInputStream;
        int parallel = Integer.parseInt(siddhiAppdist.getParallel());

        InputStream inputStream = (executionElement).getInputStream();
        if (parallel > 1 && queryElement) {
            //send to check for validity of the query type eg:join , window, pattern , sequence
            checkQueryType(inputStream);

        }


        listInputStream = (executionElement).getInputStream().getAllStreamIds();
        for (int j = 0; j < listInputStream.size(); j++) {

            String inputStreamId = listInputStream.get(j);

            //not an inner Stream
            if (!inputStreamId.contains("#")) {

                inputStreamDefinition = returnStreamDefinition(inputStreamId, siddhiApp, siddhiAppString, parallel, groupName);
                String streamConsumptionStrategy = checkQueryStrategy(inputStream, queryElement, inputStreamId, parallel);
                siddhiAppdist.setInputStream(inputStreamId, inputStreamDefinition[0], inputStreamDefinition[1], inputStreamDefinition[2], streamConsumptionStrategy);
            }


        }

        String outputStreamId = executionElement.getOutputStream().getId();

        //not an inner Stream
        if (!outputStreamId.contains("#")) {
            outputStreamDefinition = returnStreamDefinition(outputStreamId, siddhiApp, siddhiAppString, parallel, groupName);
            siddhiAppdist.setOutputStream(outputStreamId, outputStreamDefinition[0], outputStreamDefinition[1], outputStreamDefinition[2]);

        }


        return siddhiAppdist;


    }


    private String[] returnStreamDefinition(String streamId, SiddhiApp siddhiApp, String siddhiAppString, int parallel, String groupName) {

        String[] streamDefinition = new String[3];
        int[] queryContextEndIndex;
        int[] queryContextStartIndex;

        streamDefinition[2] = "true";

        if (siddhiApp.getStreamDefinitionMap().containsKey(streamId)) {


            queryContextStartIndex = siddhiApp.getStreamDefinitionMap().get(streamId).getQueryContextStartIndex();
            queryContextEndIndex = siddhiApp.getStreamDefinitionMap().get(streamId).getQueryContextEndIndex();
            streamDefinition[0] = ExceptionUtil.getContext(queryContextStartIndex, queryContextEndIndex, siddhiAppString);
            streamDefinition[1] = "Stream";
            streamDefinition[2] = isUserGivenStream(streamDefinition[0]);


        } else if (siddhiApp.getTableDefinitionMap().containsKey(streamId)) {


            AbstractDefinition tableDefinition = siddhiApp.getTableDefinitionMap().get(streamId);
            streamDefinition[1] = "InMemoryTable";

            for (int k = 0; k < tableDefinition.getAnnotations().size(); k++) {
                if (tableDefinition.getAnnotations().get(k).getName().equals("Store")) {
                    streamDefinition[1] = "Table";
                }
            }
            //need to check In-Memory or other
            if (parallel != 1 && streamDefinition[1].equals("InMemoryTable")) {

                throw new SiddhiAppValidationException("In-Memory Tables can not have parallel >1");
            }

            queryContextStartIndex = siddhiApp.getTableDefinitionMap().get(streamId).getQueryContextStartIndex();
            queryContextEndIndex = siddhiApp.getTableDefinitionMap().get(streamId).getQueryContextEndIndex();
            streamDefinition[0] = ExceptionUtil.getContext(queryContextStartIndex, queryContextEndIndex, siddhiAppString);

            if (streamDefinition[1].equals("InMemoryTable") && inmemoryMap.containsKey(streamId)) {
                if (!inmemoryMap.get(streamId).equals(groupName)) {
                    throw new SiddhiAppValidationException("Unsupported:Event Table " + streamId +
                            " In-Memory Table used in two execGroups: execGroup " + groupName + " && " + inmemoryMap.get(streamId));
                }
            } else {
                inmemoryMap.put(streamId, groupName);
            }

        } else if (siddhiApp.getWindowDefinitionMap().containsKey(streamId)) {

            if (parallel != 1) {
                throw new SiddhiAppValidationException("(Defined) Window can not have parallel >1");
            }

            queryContextStartIndex = siddhiApp.getWindowDefinitionMap().get(streamId).getQueryContextStartIndex();
            queryContextEndIndex = siddhiApp.getWindowDefinitionMap().get(streamId).getQueryContextEndIndex();
            streamDefinition[0] = ExceptionUtil.getContext(queryContextStartIndex, queryContextEndIndex, siddhiAppString);
            streamDefinition[1] = "Window";


            if (inmemoryMap.containsKey(streamId)) {
                if (!inmemoryMap.get(streamId).equals(groupName)) {
                    throw new SiddhiAppValidationException("Unsupported:(Defined) Window " + streamId +
                            " In-Memory window used in two execGroups: execGroup " + groupName + " && " + inmemoryMap.get(streamId));
                }
            } else {
                inmemoryMap.put(streamId, groupName);
            }


            //if stream definition is an inferred definition
        } else if (streamDefinition[0] == null) {

            if (siddhiAppRuntime.getStreamDefinitionMap().containsKey(streamId)) {

                streamDefinition[0] = "${" + streamId + "}" + siddhiAppRuntime.getStreamDefinitionMap().get(streamId).toString();
                streamDefinition[1] = "Stream";
                streamDefinition[2] = "false";

            } else if (siddhiAppRuntime.getTableDefinitionMap().containsKey(streamId)) {

                if (parallel != 1) {
                    throw new SiddhiAppValidationException("(In-Memory Tables can not have parallel >1");
                }

                streamDefinition[0] = siddhiAppRuntime.getTableDefinitionMap().get(streamId).toString();
                streamDefinition[1] = "InMemoryTable";
                streamDefinition[2] = "false";

            }

        }

        return streamDefinition;

    }

    private void createJsonConfigurationFile() {

        List<StrSiddhiApp> listSiddhiApps = new ArrayList<StrSiddhiApp>(distributiveMap.values());

        for (int i = 0; i < listSiddhiApps.size(); i++) {
            System.out.println(listSiddhiApps.get(i).toString());
        }

        CreateJson createJson = new CreateJson();
        try {
            createJson.writeConfiguration(listSiddhiApps, "/home/piyumi/SiddhiDecomposer/deployment.json");
        } catch (IOException e) {
            logger.error(e);
        }
    }

    private void checkQueryType(InputStream inputStream) {

        if (inputStream instanceof JoinInputStream) {
            throw new SiddhiAppValidationException("Join queries can not have parallel greater than 1  ");

        } else if (inputStream instanceof StateInputStream) {

            String type = ((StateInputStream) inputStream).getStateType().name();
            throw new SiddhiAppValidationException(type + " queries can not have parallel greater than 1  ");

        } else if (inputStream instanceof SingleInputStream) {
            List<StreamHandler> streamHandlers = ((SingleInputStream) inputStream).getStreamHandlers();

            for (int i = 0; i < streamHandlers.size(); i++) {
                if (streamHandlers.get(i) instanceof Window) {
                    throw new SiddhiAppValidationException("Window queries can not have parallel greater than 1  ");
                }
            }


        }

    }


    //TODO:if parallel=1 then -->strategy = A
    //TODO:if parallel>1 then -->if partitionedStream -->strategy=F
    //TODO:if parallel >1 then -->if not partitioned Stream --->strategy = A
    //TODO:if parallel >1 then -->if Filter and outside partition  not partitioned stream--->strategy = R
    //TODO:if parallel >1 then -->if Filter and outside partition and partioned stream ----> unsupported

    private String checkQueryStrategy(InputStream inputStream, boolean queryElement, String streamId, int parallel) {


        if (parallel > 1) {

            //if a partition
            if (!queryElement) {
                if (partitionTypeMap.containsKey(streamId)) {

                    //field grouping
                    return "FieldGrouping";
                } else {
                    //inside a partition but not a partitioned stream
                    return "All";
                }

            } else {

                return "RoundRobin";

            }


        } else {
            return "All";
        }


    }


    //usergiven source accessed by multiple siddhiApps
    private void parallelSourceStream() {

        String defaultStreamName = "TempStream";

        StrSiddhiApp strSiddhiApp;

        for (Map.Entry<String, StrSiddhiApp> stringStrSiddhiAppEntry : distributiveMap.entrySet()) {

            if (Integer.parseInt(stringStrSiddhiAppEntry.getValue().getParallel()) > 1) {
                Map<String, StrInputStream> inputStreamMap = stringStrSiddhiAppEntry.getValue().getInputStreamMap();

                for (Map.Entry<String, StrInputStream> strStreamEntry : inputStreamMap.entrySet()) {


                    if (strStreamEntry.getValue().getIsUserGiven().equals("true")) {
                        //when parallel >1 and input Stream is a source stream


                        if (distributiveMap.containsKey(appName + "00000000000000000")) {
                            strSiddhiApp = distributiveMap.get(appName + "00000000000000000");
                        } else {
                            strSiddhiApp = new StrSiddhiApp();
                            strSiddhiApp.setParallel("1");
                            //all such queries will come here
                            strSiddhiApp.setAppName(appName + "00000000000000000");
                        }


                        strSiddhiApp.setInputStream(strStreamEntry.getKey(), strStreamEntry.getValue().getDefinition(), "Stream", "True", "All");
                        strSiddhiApp.setQuery("From " + strStreamEntry.getKey() + " select * insert into " + defaultStreamName);
                        //runtimedefinition should come

                        String runTimestreamDefinition = removeMetainfoStream(strStreamEntry.getKey(), strStreamEntry.getValue().getDefinition());
                        strSiddhiApp.setOutputStream("TempStream", "${TempStream}" + runTimestreamDefinition.replaceAll(strStreamEntry.getKey(), defaultStreamName), "Stream", "false");
                        //set publishing strategy
                        inputStreamMap.get(strStreamEntry.getKey()).setIsUserGiven("false");
                        inputStreamMap.get(strStreamEntry.getKey()).setDefinition("${TempStream}" + runTimestreamDefinition);

                    }
                }


            }

        }

    }

    //if source in multiple places
    //if source and sink case
    //if partioned stream case
    //setting outputstreamStrategy


    private String removeMetainfo(ExecutionElement executionElement, String strExecutionelement) {


        for (int i = 0; i < executionElement.getAnnotations().size(); i++) {


            if (executionElement.getAnnotations().get(i).getName().toLowerCase().equals("dist")) {
                queryContextStartIndex = executionElement.getAnnotations().get(i).getQueryContextStartIndex();
                queryContextEndIndex = executionElement.getAnnotations().get(i).getQueryContextEndIndex();
                strExecutionelement = strExecutionelement.replace(ExceptionUtil.getContext(queryContextStartIndex, queryContextEndIndex, siddhiAppString), "");
            }

        }

        return strExecutionelement;
    }


    private String removeMetainfoStream(String streamId, String strStreamDefinition) {


        for (int i = 0; i < siddhiApp.getStreamDefinitionMap().get(streamId).getAnnotations().size(); i++) {
            if (siddhiApp.getStreamDefinitionMap().get(streamId).getAnnotations().get(i).getName().toLowerCase().equals("source")) {
                queryContextStartIndex = siddhiApp.getStreamDefinitionMap().get(streamId).getAnnotations().get(i).getQueryContextStartIndex();
                queryContextEndIndex = siddhiApp.getStreamDefinitionMap().get(streamId).getAnnotations().get(i).getQueryContextEndIndex();
                strStreamDefinition = strStreamDefinition.replace(ExceptionUtil.getContext(queryContextStartIndex, queryContextEndIndex, siddhiAppString), "");
            }

        }
        return strStreamDefinition;
    }


}