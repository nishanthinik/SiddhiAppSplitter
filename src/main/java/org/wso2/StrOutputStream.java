package org.wso2;

import java.util.List;

public class StrOutputStream extends StrStream{

    /* private List<StrStrategy> publishingStrategy;*/
    private String publishingStrategy;


    public StrOutputStream(String type, String definition, String isUserGiven) {
        super(type, definition, isUserGiven);

    }

    public String getPublishingStrategy() {
        return publishingStrategy;
    }

    public void setPublishingStrategy(String publishingStrategy) {
        this.publishingStrategy = publishingStrategy;
    }
    /*    public List<StrStrategy> getPublishingStrategy() {
        return publishingStrategy;
    }

    public void setPublishingStrategy(List<StrStrategy> publishingStrategy) {
        this.publishingStrategy = publishingStrategy;
    }*/
}
