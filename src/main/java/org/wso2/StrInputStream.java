package org.wso2;

public class StrInputStream extends StrStream {

    // private StrStrategy subscriptionStrategy;
    private String subscriptionStrategy;



   /* public StrInputStream(String type, String definition, boolean isUserGiven, StrStrategy subscriptionStrategy) {
        super(type, definition, isUserGiven);
        this.subscriptionStrategy = subscriptionStrategy;
    }*/

    public StrInputStream(String type, String definition, String isUserGiven, String subscriptionStrategy) {
        super(type, definition, isUserGiven);
        this.subscriptionStrategy = subscriptionStrategy;
    }

    public String getSubscriptionStrategy() {
        return subscriptionStrategy;
    }

    public void setSubscriptionStrategy(String subscriptionStrategy) {
        this.subscriptionStrategy = subscriptionStrategy;
    }
   /*    public StrStrategy getSubscriptionStrategy() {
        return subscriptionStrategy;
    }


    public void setSubscriptionStrategy(StrStrategy subscriptionStrategy) {
        this.subscriptionStrategy = subscriptionStrategy;
    }*/
}
