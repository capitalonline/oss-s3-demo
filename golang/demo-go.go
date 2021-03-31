package main

import (
	"bufio"
	"bytes"
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"io"
	"os"
)

/*

注意：当对象存储系统开启版本控制功能后，对象版本编号是一个很重要的参数，需要传递时不可使用默认值，否则可能因为系统版本问题，出现控制台数据展示异常！
	 并且请仔细阅读各个函数说明和参数案例
*/

const (
	access_key = "您的AccessKey"
	secret_key = "您的SecretKey"
	end_point  = "您的Endpoint"
	Region     = "us-east-1"
)


// TODO import Server （后续进行函数调用）

var Server *server

func init() {
	Server = &server{}
	Server.init()
}

type server struct {
	s3Client *s3.S3
	s3Sess   *session.Session
}

func (this *server) init() {
	sess, err := session.NewSession(&aws.Config{
		Credentials:      credentials.NewStaticCredentials(access_key, secret_key, ""),
		Endpoint:         aws.String(end_point),
		Region:           aws.String(Region),
		S3ForcePathStyle: aws.Bool(true),
	})
	if err != nil {
		fmt.Println(err)
	}
	this.s3Sess = sess
	this.s3Client = s3.New(sess)
}

/* 新建Bucket */
func (this *server) CreateBucket(bucketName string) bool {
	params := &s3.CreateBucketInput{
		Bucket: aws.String(bucketName),
		CreateBucketConfiguration: &s3.CreateBucketConfiguration{
			LocationConstraint: aws.String(Region),
		},
	}
	_, err := this.s3Client.CreateBucket(params)
	if err != nil {
		return false
	}
	return this.AddBucketRet(bucketName)
}

/* 删除桶 */
func (this *server) DeleteBucket(bucketName string) bool {
	// 注：删除指定的桶。只有该桶中所有的对象被删除了，该桶才能被删除。另外，只有该桶的拥有者才能删除它，与桶的访问控制权限无关
	params := &s3.DeleteBucketInput{
		Bucket: aws.String(bucketName),
	}
	_, err := this.s3Client.DeleteBucket(params)
	if err != nil {
		return false
	}
	return this.DelBucketRet(bucketName)
}

/* 获取桶信息列表 */
func (this *server) SelectBuckets() (buckets []string) {
	result, err := this.s3Client.ListBuckets(nil)
	if err == nil {
		for _, value := range result.Buckets {
			buckets = append(buckets, *value.Name)
		}
	}
	return buckets
}

/* 查看Bucket访问权限 */
func (this *server) GetBucketAcl(bucketName string) interface{} {
	params := &s3.GetBucketAclInput{
		Bucket: aws.String(bucketName),
	}
	resp, err := this.s3Client.GetBucketAcl(params)
	if err != nil {
		fmt.Println(err)
	}
	return resp
}

/*
暂时不支持
*/

/* 设置Bucket访问权限 */
func (this *server) SetBucketAcl(bucketName, ACL string) bool {
	params := &s3.PutBucketAclInput{
		Bucket: aws.String(bucketName),
		ACL:    aws.String(ACL),
	}
	_, err := this.s3Client.PutBucketAcl(params)
	if err != nil {
		fmt.Println(err)
	}
	return true
}

// TODO 问题1 web页面设置不是立即生效	问题2开启多版本，立即上传出现数据异常覆盖

/* 上传文件 (注：不开启版本控制功能，同目录同文件名会覆盖式更新, 开启后将以不同版本形式存在) */
func (this *server) UploadFile(bucketName, localFilePath, OssFilePath string) bool {
	file, err := os.Open(localFilePath)
	if err != nil {
		fmt.Println(err)
		return false
	}
	defer file.Close()
	uploader := s3manager.NewUploader(this.s3Sess)
	resp, err1 := uploader.Upload(&s3manager.UploadInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(OssFilePath),
		Body:   file,
	})
	if err1 == nil {
		return this.AddObjectRet(bucketName, OssFilePath, resp.VersionID)
	} else {
		fmt.Println(err1)
		return false
	}
}

/* 获取对象版本 (注：1.必须开启多版本控制功能  2.名称匹配不是完全匹配) */
func (this *server) ListObjectVersions(bucketName, Prefix string) (versionsInfo []*s3.ObjectVersion) {
	params := &s3.ListObjectVersionsInput{
		Bucket: aws.String(bucketName),
		Prefix: aws.String(Prefix),
	}
	resp, err := this.s3Client.ListObjectVersions(params)
	if err == nil {
		versionsInfo = resp.Versions
	}
	return
}

/* 大文件分片上传 (注：同目录同文件名会覆盖式更新) */
func (this *server) UploadLargeFile(bucketName, localFilePath, OssFilePath string, rangeSize int64, isMultiThread bool) bool {
	params1 := &s3.CreateMultipartUploadInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(OssFilePath),
	}
	respInit, err1 := this.s3Client.CreateMultipartUpload(params1)
	if err1 != nil {
		fmt.Println(err1)
		return false
	}

	upId := respInit.UploadId
	f, err2 := os.Open(localFilePath)
	if err2 != nil {
		fmt.Println(err2)
		return false
	}
	defer f.Close()

	bfRd := bufio.NewReader(f)
	buf := make([]byte, rangeSize) //TODO:一次读取多少个字节

	var partNum = 0
	var completes []*s3.CompletedPart
	////////////////// TODO: 可以选择多协程 //////////////////////////////////
	for {
		n, err3 := bfRd.Read(buf)
		if err3 == io.EOF || n == 0 {
			break
		}
		// 每次上传一个分片，每次的PartNumber都要唯一
		partNum++
		params := &s3.UploadPartInput{
			Bucket:     aws.String(bucketName),
			Key:        aws.String(OssFilePath),
			PartNumber: aws.Int64(int64(partNum)), // Required 每次的序号唯一且递增
			UploadId:   upId,                      // Required 创建context时返回的值
			Body:       bytes.NewReader(buf[:n]),  // Required 数据内容
		}

		resp2, err4 := this.s3Client.UploadPart(params)
		if err4 != nil {
			fmt.Println(err4)
			return false
		}

		var c s3.CompletedPart
		c.PartNumber = aws.Int64(int64(partNum)) // Required Etag对应的PartNumber, 上一步返回的
		c.ETag = resp2.ETag                      // Required 上传分片时返回的值 Etag
		completes = append(completes, &c)
	}

	params2 := &s3.CompleteMultipartUploadInput{
		Bucket:   aws.String(bucketName),
		Key:      aws.String(OssFilePath),
		UploadId: upId,
		MultipartUpload: &s3.CompletedMultipartUpload{
			Parts: completes,
		},
	}
	resp_comp, err := this.s3Client.CompleteMultipartUpload(params2)
	if err != nil {
		fmt.Println(err.Error())
		return false
	} else {
		return this.AddObjectRet(bucketName, OssFilePath, resp_comp.VersionId)
	}
}


/* 文件下载 */
func (this *server) DownloadVersion(bucketName, OssFilePath, localFilePath, VersionId string) bool {
	// 注：VersionId = ""  代表未开启版本控制功能的默认值 或者 开启版本控制功能的最新版本；功能开启后可以选定版本下载
	file, err := os.Create(localFilePath)
	if err != nil {
		fmt.Println(err)
		return false
	}
	defer file.Close()
	downloader := s3manager.NewDownloader(this.s3Sess)
	numBytes, err := downloader.Download(file,
		&s3.GetObjectInput{
			Bucket:    aws.String(bucketName),
			Key:       aws.String(OssFilePath),
			VersionId: aws.String(VersionId),
		})
	if err != nil && numBytes != 0 {
		fmt.Println(err)
		return false
	} else {
		return true
	}
}

/* 列举Bucket目录下所有文件列表 (注：包括子级目录文件和目录层级) */
func (this *server) ListObjectAllfiles(bucketName string) (fileArray []string) {
	params := &s3.ListObjectsV2Input{
		Bucket: aws.String(bucketName),
	}
	resp, err := this.s3Client.ListObjectsV2(params)
	if err == nil {
		for _, value := range resp.Contents {
			fileArray = append(fileArray, *value.Key)
		}
	}
	return fileArray
}

/* 按前缀罗列目录下所有一级文件列表  （注：不递归） */
func (this *server) LikeListObjectfiles(bucketName, Prefix string) (fileArray []*string) {
	// 注：例：列举前缀为”img-“的所有文件 Prefix= "img-";	log/img-的文件 Prefix= "log/img-"
	params := &s3.ListObjectsV2Input{
		Bucket: aws.String(bucketName),
		Prefix: aws.String(Prefix),
	}
	resp, err := this.s3Client.ListObjectsV2(params)
	if err == nil {
		for _, value := range resp.Contents {
			fileArray = append(fileArray, value.Key)
		}
	}
	return fileArray
}

/* 列举Bucket目录下所有一级目录列表   （注：不递归） */
func (this *server) ListObjectDirs(bucketName string) (dirArray []string) {
	params := &s3.ListObjectsV2Input{
		Bucket:    aws.String(bucketName),
		Delimiter: aws.String("/"),
	}
	resp, err := this.s3Client.ListObjectsV2(params)
	if err == nil {
		for _, value := range resp.CommonPrefixes {
			dirArray = append(dirArray, *value.Prefix)
		}
	}
	return dirArray
}

/* 列举Bucket目录下所有一级文件列表   （注：不递归） */
func (this *server) ListObjectFiles(bucketName string) (fileArray []string) {
	params := &s3.ListObjectsV2Input{
		Bucket:    aws.String(bucketName),
		Delimiter: aws.String("/"),
	}
	resp, err := this.s3Client.ListObjectsV2(params)
	if err == nil {
		for _, value := range resp.Contents {
			fileArray = append(fileArray, *value.Key)
		}
	}
	return fileArray
}

/* 列举Bucket目录下指定子级目录下的一级目录列表 （注：不递归） */
func (this *server) ListPdirDirs(bucketName, Prefix string) (dirArray []string) {
	// 例： log 获取log目录下的目录列表; imgs/one 获取imgs/one目录下的目录列表
	if !this.checkDelimiterExist(Prefix, "/") {
		Prefix += "/"
	}
	params := &s3.ListObjectsV2Input{
		Bucket:    aws.String(bucketName),
		Delimiter: aws.String("/"),
		Prefix:    aws.String(Prefix),
	}
	resp, err := this.s3Client.ListObjectsV2(params)
	if err == nil {
		for _, value := range resp.CommonPrefixes {
			dirArray = append(dirArray, *value.Prefix)
		}
	}
	return dirArray
}

/* 列举Bucket目录下指定子级目录下的一级文件列表 （注：不递归） */
func (this *server) ListPdirFiles(bucketName, Prefix string) (fileArray []string) {
	// 例: log 获取log目录下的文件列表； imgs/log 获取imgs/log目录下的文件列表
	if !this.checkDelimiterExist(Prefix, "/") {
		Prefix += "/"
	}
	params := &s3.ListObjectsV2Input{
		Bucket:    aws.String(bucketName),
		Delimiter: aws.String("/"),
		Prefix:    aws.String(Prefix),
	}
	resp, err := this.s3Client.ListObjectsV2(params)
	if err == nil {
		for _, value := range resp.Contents {
			fileArray = append(fileArray, *value.Key)
		}
	}
	return fileArray
}

/* 路径标识符比对 */
func (this *server) checkDelimiterExist(data, key string) (ret bool) {
	for _, word := range data {
		ret = string(word) == key
	}
	return ret
}

// TODO 不指定版本号删除 	问题1 只是标记删除；问题2 默认隐藏其他版本对象

/*  删除桶下的指定版本号对象 */
func (this *server) DeleteObjectVersion(bucketName, OssFilePath, VersionId string) bool {
	// 注：VersionId = ""  代表未开启版本控制功能的默认值 或者 开启版本控制功能的最新版本；功能开启后可以选定版本下载
	_, err := this.s3Client.DeleteObject(&s3.DeleteObjectInput{
		Bucket:    aws.String(bucketName),
		Key:       aws.String(OssFilePath),
		VersionId: aws.String(VersionId),
	})
	if err != nil {
		fmt.Println(err)
		return false
	}
	return this.DelObjectRet(bucketName, OssFilePath, VersionId)
}

//TODO 无版本信息删除开启版本功能的对象数据会出现数据异常
// 参数为多个相同对象名称会出现一条数据删除多次记录

/* 模糊匹配批量删除同级目录下文件 (开启版本功能删除最新版对象，否则删除唯一对象)*/
func (this *server) DeleteMatchObjects(bucketName, Prefix string) (fileArray []string) {
	// 注：批量删除bucket1下的以2017-05为前缀的对象  例：Prefix= '2017‐05'
	// 注： 返回删除成功文件名列表
	objectsInfo := this.LikeListObjectfiles(bucketName, Prefix)
	deletes := make([]*s3.ObjectIdentifier, 0)
	for _, value := range objectsInfo {
		term := &s3.ObjectIdentifier{Key: value}
		deletes = append(deletes, term)
	}
	params := &s3.DeleteObjectsInput{
		Bucket: aws.String(bucketName),
		Delete: &s3.Delete{
			Objects: deletes,
			Quiet: aws.Bool(false),
		},
	}
	resp, err := this.s3Client.DeleteObjects(params)
	if err == nil {
		for _, value := range resp.Deleted {
			fileArray = append(fileArray, *value.Key)
		}
	} else {
		fmt.Println(err)
	}
	return
}


/* 模糊匹配且区分版本批量删除同级目录下文件 */
func (this *server) DeleteMatchObjectsFocusOnVersionId(bucketName, Prefix string) (fileArray []string) {
	// 注：批量删除bucket1下的以2017-05为前缀的对象  例：Prefix= '2017‐05'
	// 注： 返回删除成功文件名列表
	versionsInfo := this.ListObjectVersions(bucketName, Prefix)
	deletes := make([]*s3.ObjectIdentifier, 0)
	for _, value := range versionsInfo {
		term := &s3.ObjectIdentifier{Key: value.Key}
		if value.VersionId != nil {
			term.VersionId = value.VersionId
		}
		deletes = append(deletes, term)
	}
	params := &s3.DeleteObjectsInput{
		Bucket: aws.String(bucketName),
		Delete: &s3.Delete{
			Objects: deletes,
			Quiet: aws.Bool(false),
		},
	}
	resp, err := this.s3Client.DeleteObjects(params)
	if err == nil {
		for _, value := range resp.Deleted {
			fileArray = append(fileArray, *value.Key)
		}
	} else {
		fmt.Println(err)
	}
	return
}

/* 批量删除 指定（对象名+版本号）文件 */
func (this *server) DeleteObjects(bucketName string, deletes []*s3.ObjectIdentifier) (fileArray []string) {
	// 注：开启版本控制功能，需传递对象版本编号
	// 注：deletes = [{Key:xxx, VersionId;xxx}, ...]
	// 注： 返回删除成功文件名列表
	params := &s3.DeleteObjectsInput{
		Bucket: aws.String(bucketName),
		Delete: &s3.Delete{
			Objects: deletes,
			Quiet: aws.Bool(false),
		},
	}
	resp, err := this.s3Client.DeleteObjects(params)
	if err == nil {
		for _, value := range resp.Deleted {
			fileArray = append(fileArray, *value.Key)
		}
	} else {
		fmt.Println(err)
	}
	return
}

/* 判断对象删除结果 */
func (this *server) DelObjectRet(bucketName, OssFilePath, VersionId string) bool {
	err := this.s3Client.WaitUntilObjectNotExists(&s3.HeadObjectInput{
		Bucket:    aws.String(bucketName),
		Key:       aws.String(OssFilePath),
		VersionId: aws.String(VersionId),
	})
	return err == nil
}

/* 判断对象添加结果 */
func (this *server) AddObjectRet(bucketName, OssFilePath string, VersionId *string) bool {
	var VersionIdstr string
	if VersionId != nil {
		VersionIdstr = *VersionId
	}
	err := this.s3Client.WaitUntilObjectExists(&s3.HeadObjectInput{
		Bucket:    aws.String(bucketName),
		Key:       aws.String(OssFilePath),
		VersionId: aws.String(VersionIdstr),
	})
	return err == nil
}

/* 判断桶的删除结果 */
func (this *server) DelBucketRet(bucketName string) bool {
	err := this.s3Client.WaitUntilBucketNotExists(&s3.HeadBucketInput{
		Bucket: aws.String(bucketName),
	})
	return err == nil
}

/* 判断桶的添加结果 */
func (this *server) AddBucketRet(bucketName string) bool {
	err := this.s3Client.WaitUntilBucketExists(&s3.HeadBucketInput{
		Bucket: aws.String(bucketName),
	})
	return err == nil
}

/* Bucket内部文件拷贝 (注：不开启多版本控制功能同目录同文件名会覆盖式更新， 否则会以不同版本形式存在) */
func (this *server) CopyObject(NewBucketName, OldBucketName, NewFile, CopySource string) bool {
	if string(CopySource[0]) != "/" {
		CopySource = "/" + CopySource
	}
	params := &s3.CopyObjectInput{
		Bucket:     aws.String(NewBucketName),
		Key:        aws.String(NewFile),
		CopySource: aws.String(OldBucketName + CopySource),
	}
	resp, err := this.s3Client.CopyObject(params)
	if err == nil {
		// 版本控制功能 - 若开启则返回相应编号，否则为空指针
		return this.AddObjectRet(NewBucketName, NewFile, resp.VersionId)
	} else {
		return false
	}
}

/* 查看文件访问权限 */
func (this *server) GetObjectAcl(bucketName, OssFilePath string, VersionId *string) *s3.GetObjectAclOutput {
	var VersionIdStr string
	if VersionId != nil {
		VersionIdStr = *VersionId
	}
	params := &s3.GetObjectAclInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(OssFilePath),
		VersionId: aws.String(VersionIdStr),
	}
	resp, err := this.s3Client.GetObjectAcl(params)
	if err != nil {
		fmt.Println(err)
	}
	return resp
}

/*
暂时不支持
*/

/* 设置文件访问权限 */
func (this *server) PutObjectAcl(bucketName, OssFilePath, ACL string) bool {
	params := &s3.PutObjectAclInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(OssFilePath),
		ACL:    aws.String(ACL),
	}
	resp, err := this.s3Client.PutObjectAcl(params)
	if err == nil {
		fmt.Println(resp)
	}
	return true
}
