package com.cds.app;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.Bucket;
import com.amazonaws.services.s3.model.GeneratePresignedUrlRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.TransferManagerBuilder;
import com.amazonaws.services.s3.transfer.Upload;
import com.amazonaws.util.StringUtils;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.net.URL;
import java.text.MessageFormat;
import java.util.List;
import java.util.Locale;
import java.util.Random;

/**
 * Hello world!
 */
public class App {
    /**
     * ACCESSKEY
     */
    private static final String accessKey = "";
    /**
     * SECRETKEY
     */
    private static final String secretKey = "";

    private static final String hostname = "http://oss-cnbj01.cdsgss.com"; // 北京节点
    /**
     * 存储桶名称
     */
    private static final String BUCKET_NAME = getRandomString(5).toLowerCase(Locale.ROOT);

    private static final String objectKey = "cds-oss-java.iml"; // demo.txt

    private static final String file_path = "./cds-oss-java.iml";
    /**
     * 创建s3访问凭证对象
     */
    private static final BasicAWSCredentials awsCreds = new BasicAWSCredentials(accessKey, secretKey);

    public static void main(String[] args) {
        //新方法
        ClientConfiguration clientConfig = new ClientConfiguration();
        clientConfig.setSignerOverride("S3SignerType");//凭证验证方式
        AmazonS3 s3Client = AmazonS3ClientBuilder.standard()
                .withCredentials(new AWSStaticCredentialsProvider(awsCreds))
                .withClientConfiguration(clientConfig)
                // 设置节点url
                .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(hostname, ""))
                .withPathStyleAccessEnabled(true)
                .build();
        /**
         * 创建桶
         */
        System.out.println(MessageFormat.format("=============新建桶名称：{0}", BUCKET_NAME));
        addBucket(s3Client);
        /**
         * 查询节点下的全部桶的列表
         */
        System.out.println(MessageFormat.format("=============开始查询节点下的桶列表，当前节点为{0}", hostname));
        listBuckets(s3Client);
        System.out.println("===============查询节点下的桶列表结束");
        /**
         * 上传对象文件
         */
        System.out.println(MessageFormat.format("==============往桶里面上传对象，桶名称：{0}", BUCKET_NAME));
        upObjectByBurst(s3Client);
        System.out.println("==============往桶里面上传对象完成");
        /**
         * 查询桶中的对象
         */
        System.out.println(MessageFormat.format("================查询桶里面的对象，桶名称：{0}", BUCKET_NAME));
        getObject(s3Client);
        System.out.println("===============查询桶里面对象完成");

        /**
         * 给对象生成下载链接
         */
        System.out.println("=============给对象生成下载链接");
        generatePresignedUrl(s3Client);

//        System.out.println( "Hello World!" );
    }

    /**
     * 获取当前节点下全部的桶
     *
     * @param client
     */
    static void listBuckets(AmazonS3 client) {
        List<Bucket> buckets = client.listBuckets();
        for (Bucket bucket : buckets) {
            System.out.println(bucket.getName() + "\t" +
                    StringUtils.fromDate(bucket.getCreationDate()));
        }
    }

    /**
     * 新建一个桶
     *
     * @param client
     */
    static void addBucket(AmazonS3 client) {
        Bucket bucket = client.createBucket(BUCKET_NAME);
        System.out.println(bucket.getName());
    }

    /**
     * 判断一个桶是否存在
     *
     * @param client
     */
    static void doesBucketExist(AmazonS3 client) {
        boolean exists = client.doesBucketExistV2(BUCKET_NAME);
        System.out.println("查询桶是否存在" + BUCKET_NAME + "\t:" + exists);
    }

    /**
     * 删除一个桶
     *
     * @param client
     */
    static void DelBucket(AmazonS3 client) {
        client.deleteBucket(BUCKET_NAME);
        System.out.println("删除一个桶" + BUCKET_NAME);
    }

    /**
     * 上传字符串
     *
     * @param client
     */
    static void upObjectForStream(AmazonS3 client) {
        String content = "Object Content";
        client.putObject(BUCKET_NAME, objectKey, new ByteArrayInputStream(content.getBytes()), null);
        System.out.println("上传字符串到桶" + BUCKET_NAME);
    }

    /**
     * 上传本地文件
     *
     * @param client
     */
    static void upObjectByLocalFile(AmazonS3 client) {
        client.putObject(BUCKET_NAME, objectKey, new File("localFile"));
        System.out.println("上传本地文件到桶" + BUCKET_NAME);
    }

    /**
     * 分片上传文件
     *
     * @param client
     */
    static void upObjectByBurst(AmazonS3 client) {
        File f = new File(file_path);
        TransferManager xfer_mgr = TransferManagerBuilder.standard()
                .withS3Client(client)
                .withMultipartUploadThreshold((long) (5 * 1024 * 1025)) // 分片大小5MB
                .build();
        try {
            Upload xfer = xfer_mgr.upload(BUCKET_NAME, f.getName(), f);
            xfer.waitForCompletion(); // 等待上传完成
            System.out.println("分片上传文件到桶" + BUCKET_NAME + xfer);
        } catch (AmazonServiceException e) {
            System.err.println(e.getErrorMessage());
            System.exit(1);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        //xfer_mgr.shutdownNow(); // 显示关闭TransferManager
    }

    /**
     * 列出Bucket中的对象
     *
     * @param client
     */
    static void getObject(AmazonS3 client) {
        ObjectListing objects = client.listObjects(BUCKET_NAME);
        for (S3ObjectSummary s3ObjectSummary : objects.getObjectSummaries()) {
            System.out.println(MessageFormat.format("对象名称{0},修改日期{1}", s3ObjectSummary.getKey(), s3ObjectSummary.getLastModified()));
        }
    }

    /**
     * 删除object对象
     *
     * @param client
     */
    static void delObject(AmazonS3 client) {
        client.deleteObject(BUCKET_NAME, objectKey);
        System.out.println("删除指定桶中的对象" + BUCKET_NAME);
    }

    /**
     * 生成共享下载URL
     *
     * @param client
     */
    static void generatePresignedUrl(AmazonS3 client) {
        GeneratePresignedUrlRequest request = new GeneratePresignedUrlRequest(BUCKET_NAME, objectKey);

        URL url = client.generatePresignedUrl(request);
        // request.setExpiration(date); // 设置过期时间 当到达该时间点时 URL就会过期 其他人不能访问该对象
        System.out.println("生成下载链接" + url);
    }

    //length用户要求产生字符串的长度
    public static String getRandomString(int length) {
        String str = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
        Random random = new Random();
        StringBuffer sb = new StringBuffer();
        for (int i = 0; i < length; i++) {
            int number = random.nextInt(62);
            sb.append(str.charAt(number));
        }
        return sb.toString();
    }
}
