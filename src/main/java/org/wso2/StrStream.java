package org.wso2;

public abstract class StrStream {
    String type;
    String definition;
    boolean ispartitioned;
    String isUserGiven;

    public boolean isIspartitioned() {
        return ispartitioned;
    }

    public void setIspartitioned(boolean ispartitioned) {
        this.ispartitioned = ispartitioned;
    }

    public String getIsUserGiven() {
        return isUserGiven;
    }

    public void setIsUserGiven(String isUserGiven) {
        this.isUserGiven = isUserGiven;
    }

    public StrStream(String type, String definition, String isUserGiven) {

        this.type = type;
        this.definition = definition;
        this.isUserGiven = isUserGiven;
    }

    public String getType() {
        return type;

    }

    public void setType(String type) {
        this.type = type;
    }

    public String getDefinition() {
        return definition;
    }

    public void setDefinition(String definition) {
        this.definition = definition;
    }
}
