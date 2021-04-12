// 使用nodejs测试s3api
// 参考地址：
// https://docs.aws.amazon.com/AWSJavaScriptSDK/latest/AWS/S3.html

//进行初始化client操作
const AWS = require('aws-sdk')

// endpoint
const Endpoint ="http://oss-fra.cdsgss.com";

AWS.config.update({
    "accessKeyId" : "",
    "secretAccessKey" : "",
    "endpoint" : Endpoint,
    "sslEnabled" : false,
    "s3ForcePathStyle" : true
});

//构建aws对象
const s3 = new AWS.S3();  
s3.endpoint = new AWS.Endpoint(Endpoint); // 法兰克福节点
const fs = require('fs')

function listBucket() {
    //列举用户拥有的容器
    s3.listBuckets(function(err, data) {
        if (err) {console.log(err)}
        else {console.log(data)}
    })
}

function createNewBucket(bucketName) {
    //创建容器,默认为私有空间
    const p = {
        Bucket : bucketName
    };
    s3.createBucket(p, function(err, data) {
        if (err) {console.log(err, err.stack)}
        else {console.log("容器", bucketName, '创建成功，地址是', data.Location)}
    })
}

function createBucketPublicReadWrite(bucketName) {
    //ACL: private | public-read | public-read-write | authenticated-read
    var p = {
        Bucket: bucketName,
        // ACL: 'public-read-write'  //创建后页面仍显示的私有空间？
    }
    s3.ACL = 'public-read-write'
    s3.createBucket(p, (err, data) => {
        console.log(data, err)
        if (err) {console.log(err, err.stack)}
        else {console.log("容器", bucketName, '创建成功，地址是', data.Location)}
    })
}

//上传对象
function uploadFile(bucketname, fileName) {   
    const fileContent = fs.readFileSync(fileName)
    const params = {
        Bucket : bucketname,
        Key : fileName,
        Body : fileContent
    }
    s3.upload(params, function(err, data) {
        if (err) {console.log(err, err.stack)}
        else {console.log(fileName, "已经上传到容器",bucketname,'中，对象的地址是', data.Location)}
    })
}
//下载对象
function downloadFile(bucketName, fileName, downPath) {
    var params = {
        Bucket : bucketName,
        Key : fileName
    }
    s3.getObject(params, function(err, data) {
        if (err) {console.error(err)}
        else {
            fs.writeFileSync(downPath, data.Body.toString());
            console.log(downPath, ' has been created')
        }
    })
}
//获取对象列表
function listFile(bucketName) {
    var params = {
        Bucket : bucketName,
        Delimiter : ""
    }
    var files = []
    s3.listObjects(params, function(err, data) {
        if (err) {console.log(err, err.stack)}
        else {
            var all = data["Contents"]
            for(var x in all) {
                s3obj = all[x]['Key']
                files.push(s3obj)
            }
            console.log(bucketName, "中的对象列表：\n", files)}
    })
}
//删除对象
function deleteFile(bucketName, fileName) {
    var params = {
        Bucket : bucketName,
        Key : fileName
    }
    s3.deleteObject(params, function(err, data) {
        if (err) {console.log(err)}
        else {console.log(bucketName,"中的对象",fileName,"已经删除")}
    })
}
//获取容器acl信息
function getBucketAclInfo(bucketName) {
    var params = {
        Bucket : bucketName
    }
    s3.getBucketAcl(params, function(err, data) {
        if (err) {console.error(err)}
        else {console.log(bucketName,"的权限是", data['Grants'])}
    })
}
//分片上传大文件
function partUploadFile(bucketName, objName, filePath) {
    let uploader = require('s3-upload-streams')
    const partSize = 5*1024*1024
    let s3Uploader = new uploader(s3, bucketName, partSize, 1000)
    let stream = fs.createReadStream(filePath)
    let uploadIdPromise = s3Uploader.startUpload({ Key: objName }, stream, { orginalPath: filePath});
    stream.on('end', () => {
        uploadIdPromise
        .then(uploadId => s3Uploader.completeUpload(uploadId))
        .then((metadata) => {
            console.log('Uploaded', metadata.additionalMetadata.orginalPath ,'to', metadata.location);
        })
        .catch(err => console.log(err))
    })
}
//判断文件是否存在
function ifObjExist(bucketName, objName) {
    var params = {
        Bucket : bucketName,
        Key : objName
    }
    s3.headObject(params, function(err, data) {
        if (err) {console.log(bucketName, "中没有对象", objName)}
        else {console.log(bucketName, "中的对象", objName,"存在，headObject()返回信息为：", data)}
    })
}
//拷贝对象
function copyObj(srcBucket, srcObj, dstBucket, dstObj) {
    params = {
        Bucket : dstBucket,
        CopySource : srcBucket + '/' + srcObj,
        Key : dstObj
    }
    s3.copyObject(params, function(err, data) {
        if (err) {console.log(err, err.stack)}
        else {console.log(data)}
    })
}
//获取对象的acl信息
function getObjAclInfo(bucketName, objName) {
    var params = {
        Bucket : bucketName,
        Key : objName
    }
    s3.getObjectAcl(params, function(err, data) {
        if (err) {console.log(err, err.stack)}
        else {console.log(bucketName,"中的对象",objName, "的访问权限是", data.Grants)}
    })
}
//获取对象私有连接
function getObjPrivateUrl(bucketName, objName) {
    var params = {
        Bucket : bucketName,
        Key : objName,
        Expires : 60*60
    }
    const url = s3.getSignedUrl('getObject', params)
    console.log(url)
}
//对象添加标签
function addFileTag(bucketName, objName) {   
    var p = {
        Bucket: bucketName, /* required */
        Key: objName, /* required */
        Tagging: { /* required */
            TagSet: [ /* required */
                {
                    Key: 'key', /* required */
                    Value: 'value' /* required */
                },
            /* more items */
            ]
        },
    }
    s3.putObjectTagging(p, (err, data) => {
        if (err) {console.log(err, err.stack)}
        else console.log("对象标签添加成功")
    })
}
function getFileTag(bucketName, objName) {
    var p = {
        Bucket : bucketName,
        Key : objName
    }
    s3.getObjectTagging(p, (err, data)=> {
        if (err) {console.log(err, err.stack)}
        else console.log(bucketName , "中的对象", objName, "的标签信息：", data)
    })
}
function delFileTag(bucketName, objName) {
    var p = {
        Bucket : bucketName,
        Key : objName
    }
    s3.deleteObjectTagging(p, (err, data) => {
        if (err) {console.log(err, err.stack)}
        else console.log(bucketName , "中的对象", objName, "标签删除成功")
    })
}
function addBucketTag(bucketName) {
    var p = {
        Bucket : bucketName,
        Tagging: {
            TagSet: [
                {
                    Key: "Key1", 
                    Value: "Value1"
                }, 
               {
                    Key: "Key2", 
                    Value: "Value2"
                }
            ]
        }
    }
    s3.putBucketTagging(p, (err, data) => {
        if (err) {console.log(err, err.stack)}
        else console.log("容器标签添加成功")
    })
}
function getBucketTag(bucketName) {
    var p = {
        Bucket : bucketName
    }
    s3.getBucketTagging(p, (err, data) => {
        if (err) {console.log(err, err.stack)}
        else console.log(bucketName , "的标签信息：", data)
    })
}
function delBucketTag(bucketName) {
    var p = {
        Bucket : bucketName
    }
    s3.deleteBucketTagging(p, (err, data) => {
        if (err) {console.log(err, err.stack)}
        else console.log(bucketName , "标签删除成功")
    })
}
// 获取桶列表
listBucket()
// 创一个公共桶
// createBucketPublicReadWrite('sdsd');

// addBucketTag("aaa")
// getBucketTag("aaa")
// delBucketTag("aaa")
// addFileTag("aaa", "a.txt")
// delFileTag("aaa", "a.txt")
// getFileTag("aaa", "a.txt")
// getObjPrivateUrl("nodejsbucket", "bigfile100M")
// getObjAclInfo("nodejsbucket", "bigfile100M")
// copyObj("video-in", "JS2m57s.mp4", "nodejsbucket", "copybyjs.mp4")
// ifObjExist("nodejsbucket", "aaa")
// partUploadFile("nodejsbucket", "bigfile100M", "G:\\wordfortest\\file-100M.zip")
// createNewBucket("nodejs")
// createBucketPublicReadWrite("jspublic")
// listBucket()
// uploadFile("nodejsbucket", "nodejsup")
// downloadFile("nodejsbucket", "nodejsup", "nodejsdown.txt")
// listFile("www")
// deleteFile("nodejsbucket", "nodejsup")
// getBucketAclInfo("nodejsbucket")
