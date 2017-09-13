package org.techstars;

import java.util.Map;

/**
 * Created by akamlani on 8/14/17.
 */
public class CredentialParams {

    private Map<String, String> aws;

    public String aws_endpoint;
    public String aws_access_key_id;
    public String aws_secret_access_key;

    public Map<String, String> getAWS() {
        return aws;
    }
    public void setAWS(Map<String, String> aws) {
        this.aws = aws;
        this.aws_endpoint           = aws.get("aws_endpoint");
        this.aws_access_key_id      = aws.get("aws_access_key_id");
        this.aws_secret_access_key  = aws.get("aws_secret_access_key");
    }

}
