package org.wso2;

import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;


public class StrSiddhiApp {

    private Map<String,StrInputStream> inputStreamMap;
    private Map<String,StrOutputStream> outputStreamMap;
    private List<StrQuery> queryList;
    private String appName;
    private String parallel;

    public StrSiddhiApp(){
        this.inputStreamMap = new LinkedHashMap< String,StrInputStream>();
        this.outputStreamMap = new LinkedHashMap<String, StrOutputStream>();
        this.queryList = new LinkedList<StrQuery>();

    }


    public String getAppName() {
        return appName;
    }

    public void setAppName(String appName) {
        this.appName = appName;
    }

    public String getParallel() {
        return parallel;
    }

    public void setParallel(String parallel) {
        this.parallel = parallel;
    }

    public void setInputStream(String key, String inputStream,String type, String isUsergiven, String publishingStrategy) {

        inputStreamMap.put(key, new StrInputStream(type,inputStream,isUsergiven,publishingStrategy));
    }

    public void setOutputStream(String key, String outputStream,String type, String  isUsergiven) {

        outputStreamMap.put(key, new StrOutputStream(type,outputStream, isUsergiven));

    }

    public Map<String,StrInputStream> getInputStreamMap() {
        return inputStreamMap;
    }

    public void setInputStreamMap(Map<String,StrInputStream> inputStreamMap) {
        this.inputStreamMap = inputStreamMap;
    }

    public Map<String,StrOutputStream> getOutputStreamMap() {
        return outputStreamMap;
    }

    public void setOutputStreamMap(Map<String,StrOutputStream> outputStreamMap) {
        this.outputStreamMap = outputStreamMap;
    }

    public void setQuery(String query ) {
        queryList.add(new StrQuery(query));
    }


    public List<StrQuery> getQueryList() {
        return queryList;
    }

    public void setQueryList(List<StrQuery> queryList) {
        this.queryList = queryList;
    }

    @Override
    public String toString() {

        StringBuilder stringBuilder = new StringBuilder("@App:name(\"" + appName + "\") \n");
        String s;

        for (Map.Entry<String,StrInputStream> entry : inputStreamMap.entrySet()) {

            s = entry.getValue().getDefinition();
            if (s != null) {
                stringBuilder.append(s).append(";\n");
            }


        }


        for (Map.Entry<String,StrOutputStream> entry : outputStreamMap.entrySet()) {

            s = entry.getValue().getDefinition();
            if (s != null) {
                stringBuilder.append(s).append(";\n");
            }


        }


        for (int i = 0; i < queryList.size(); i++) {
            stringBuilder.append(queryList.get(i).getQuery()).append(";\n");
        }

        return stringBuilder.toString();
    }





    public String toJsonString() {
        StringBuilder stringBuilder = new StringBuilder();
        String s;

        for (Map.Entry<String,StrInputStream> entry : inputStreamMap.entrySet()) {

            s = entry.getValue().getDefinition();
            if (s != null) {
                stringBuilder.append(s).append("; ");
            }


        }


        for (Map.Entry<String,StrOutputStream> entry : outputStreamMap.entrySet()) {

            s = entry.getValue().getDefinition();
            if (s != null) {
                stringBuilder.append(s).append("; ");
            }


        }


        for (int i = 0; i < queryList.size(); i++) {
            stringBuilder.append(queryList.get(i).getQuery()).append("; ");
        }

        return stringBuilder.toString();

    }


}
