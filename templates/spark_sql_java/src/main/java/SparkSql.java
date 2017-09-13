package org.techstars;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import org.apache.spark.SparkContext;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.api.java.JavaRDD;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;

public class SparkSql {
    final static Logger log = LoggerFactory.getLogger(SparkSql.class);
    final static String warehouse_dir = "/tmp/spark-warehouse";


    public static void main(String[] args) throws IOException{
        CredentialParams credentials = readCredentials();

        //Note that Kyro Serialization has difficulties in working with Off-Heap Data Structures in ND4J
        //Uses Spark 2.x Session, instead of just SparkContext
        SparkSession spark = SparkSession.builder()
                .appName("Neural Network Spark Session")
                .master("local[*]")     //spark:IP:7073; yarn; mesos
                .config("spark.sql.warehouse.dir", warehouse_dir)
                .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                .config("spark.kryo.registrator", "org.nd4j.Nd4jRegistrator")
                .config("fs.s3a.endpoint",   credentials.aws_endpoint)
                .config("fs.s3a.access.key", credentials.aws_access_key_id)
                .config("fs.s3a.secret.key", credentials.aws_secret_access_key)
                //.enableHiveSupport()
                .getOrCreate();

        SparkContext sc = spark.sparkContext();
        //Programmatically turn off verbose messages (dependent on log4j)
        sc.setLogLevel("ERROR");

        //log data
//        log.info(credentials.getAWS().toString());
//        log.info(spark.version());
//        log.info(spark.conf().getAll().toString());

        System.out.println(credentials.getAWS().toString());
        System.out.println(spark.version());
        System.out.println(spark.conf().getAll().toString());

    }



    /**
     * Creates in AWS Credentials
     */
    private static CredentialParams readCredentials() throws IOException {
        //ClassPathResource res = new ClassPathResource("config/credentials.yaml");
        //File f = res.getFile();
        String s = "/Users/akamlani/projects/datascience/workshops/skymind/sparksql/src/main/resources/config/credentials.yml";
        File f = new File(s);
        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        CredentialParams credentials = mapper.readValue(f, CredentialParams.class);
        return credentials;
    }


}

