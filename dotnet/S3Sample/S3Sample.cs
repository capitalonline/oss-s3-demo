using System;
using System.IO;
using Amazon.S3;
using Amazon.S3.Model;

namespace GettingStartedGuide
{
    class S3Sample
    {
        static string bucketName = "fra-bucket-write"; // 桶名称
        static string keyName = "eula.1031.txt"; // 对象名称
        static string filePath = "D:\\eula.1031.txt"; // 文件路径
        static IAmazonS3 client;
        // 对象 存储 AK,SK
        private static readonly string awsAccessKey = "";
        private static readonly string SecretKey = "";

        //提供最基本的一个配置
        public static AmazonS3Config config = new AmazonS3Config()
        {
            //这个地址是存储所在的节点域名
            ServiceURL = "https://oss-fra.cdsgss.com", // 法兰克福
            ForcePathStyle = true // 必须为true
        };

        public static void Main(string[] args)
        {
            using (client = new AmazonS3Client(awsAccessKey, SecretKey, config))
            {

                 Console.WriteLine("Listing buckets");
                 ListingBuckets();

                 Console.WriteLine("Creating a bucket");
                 CreateABucket();

                 Console.WriteLine("Writing an object");
                 WritingAnObject();

                Console.WriteLine("update file to object");
                UpdateFileToObject();

                Console.WriteLine("Reading an object");
                ReadingAnObject();

                Console.WriteLine("Deleting an object");
                DeletingAnObject();

                Console.WriteLine("Listing objects");
                ListingObjects();
            }

            Console.WriteLine("Press any key to continue...");
            Console.ReadKey();
        }

        static void ListingBuckets()
        {
            try
            {
                ListBucketsResponse response = client.ListBuckets();
                foreach (S3Bucket bucket in response.Buckets)
                {
                    Console.WriteLine("You own Bucket with name: {0}", bucket.BucketName);
                }
            }
            catch (AmazonS3Exception amazonS3Exception)
            {
                Console.WriteLine("An Error, number {0}, occurred when listing buckets with the message '{1}", amazonS3Exception.ErrorCode, amazonS3Exception.Message);
            }
        }

        static void CreateABucket()
        {
            try
            {
                PutBucketRequest request = new PutBucketRequest();
                request.BucketName = bucketName;
                request.CannedACL = S3CannedACL.Private;
                client.PutBucket(request);
            }
            catch (AmazonS3Exception amazonS3Exception)
            {
                Console.WriteLine("An Error, number {0}, occurred when creating a bucket with the message '{1}", amazonS3Exception.ErrorCode, amazonS3Exception.Message);
            }
        }

        static void WritingAnObject()
        {
            try
            {
                // simple object put
                PutObjectRequest request = new PutObjectRequest()
                {
                    ContentBody = "this is a test",
                    BucketName = bucketName,
                    Key = keyName,

                };

                PutObjectResponse response = client.PutObject(request);

                // put a more complex object with some metadata and http headers.
                PutObjectRequest titledRequest = new PutObjectRequest()
                {
                    BucketName = bucketName,
                    Key = keyName
                };
                titledRequest.Metadata.Add("title", "the title");

                client.PutObject(titledRequest);
            }
            catch (AmazonS3Exception amazonS3Exception)
            {
                if (amazonS3Exception.ErrorCode != null &&
                    (amazonS3Exception.ErrorCode.Equals("InvalidAccessKeyId") ||
                    amazonS3Exception.ErrorCode.Equals("InvalidSecurity")))
                {
                    Console.WriteLine("Please check the provided AWS Credentials.");
                    Console.WriteLine("If you haven't signed up for Amazon S3, please visit http://aws.amazon.com/s3");
                }
                else
                {
                    Console.WriteLine("An error occurred with the message '{0}' when writing an object", amazonS3Exception.Message);
                }
            }
        }

        static void UpdateFileToObject()
        {
            try
            {
                // simple object put
                PutObjectRequest request = new PutObjectRequest()
                {
                    BucketName = bucketName,
                    FilePath = filePath,
                };
                PutObjectResponse response = client.PutObject(request);
            }
            catch (AmazonS3Exception amazonS3Exception)
            {
                Console.WriteLine("An error occurred with the message '{0}' when writing an object", amazonS3Exception.Message);
            }
        }

        static void ReadingAnObject()
        {
            try
            {
                GetObjectRequest request = new GetObjectRequest()
                {
                    BucketName = bucketName,
                    Key = keyName
                };

                using (GetObjectResponse response = client.GetObject(request))
                {
                    string title = response.Metadata["x-object-tagging"];
                    Console.WriteLine("The object's tag is {0}", title);
                    string dest = Path.Combine(Environment.GetFolderPath(Environment.SpecialFolder.Desktop), keyName);
                    if (!File.Exists(dest))
                    {
                        response.WriteResponseStreamToFile(dest);
                    }
                }
            }
            catch (AmazonS3Exception amazonS3Exception)
            {
                Console.WriteLine("An error occurred with the message '{0}' when reading an object", amazonS3Exception.Message);
            }
        }

        static void DeletingAnObject()
        {
            try
            {
                DeleteObjectRequest request = new DeleteObjectRequest()
                {
                    BucketName = bucketName,
                    Key = keyName
                };

                client.DeleteObject(request);
            }
            catch (AmazonS3Exception amazonS3Exception)
            {
                Console.WriteLine("An error occurred with the message '{0}' when deleting an object", amazonS3Exception.Message);
            }
        }

        static void ListingObjects()
        {
            try
            {
                ListObjectsRequest request = new ListObjectsRequest();
                request.BucketName = bucketName;
                ListObjectsResponse response = client.ListObjects(request);
                foreach (S3Object entry in response.S3Objects)
                {
                    Console.WriteLine("key = {0} size = {1}", entry.Key, entry.Size);
                }

                // list only things starting with "foo"
                request.Prefix = "eula";
                response = client.ListObjects(request);
                foreach (S3Object entry in response.S3Objects)
                {
                    Console.WriteLine("key = {0} size = {1}", entry.Key, entry.Size);
                }

                // list only things that come after "bar" alphabetically
                request.Prefix = null;
                request.Marker = "bar";
                response = client.ListObjects(request);
                foreach (S3Object entry in response.S3Objects)
                {
                    Console.WriteLine("key = {0} size = {1}", entry.Key, entry.Size);
                }

                // only list 3 things
                request.Prefix = null;
                request.Marker = null;
                request.MaxKeys = 3;
                response = client.ListObjects(request);
                foreach (S3Object entry in response.S3Objects)
                {
                    Console.WriteLine("key = {0} size = {1}", entry.Key, entry.Size);
                }
            }
            catch (AmazonS3Exception amazonS3Exception)
            {
                Console.WriteLine("An error occurred with the message '{0}' when listing objects", amazonS3Exception.Message);
            }
        }
    }
}