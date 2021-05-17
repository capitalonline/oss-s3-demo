# coding=utf-8
"""
Python 操作 OSS Demo

此版本的Python SDK适用于Python 2.7
    1.通过pip安装， pip install boto3
    2.通过源码安装。
        git clone https://github.com/boto/boto3.git && cd boto3 && sudo python setup.py install

备注说明：
    对象存储的存储空间（Bucket）本身是扁平结构的，并没有文件夹或目录的概念。用户可以通过在文件名里加入”/“来模拟文件夹。
"""

from boto3.session import Session
import boto3
from botocore.exceptions import ClientError
from threading import Thread

ACLP = 'private'  # 私有桶
ACLPR = 'public‐read'  # 公共读桶
ACLPRW = 'public‐read‐write'  # 公共读写桶
ACLAR = 'authenticated‐read'  # 认证读桶


class ActionDemo:
    def __init__(self):
        self.access_key = '"您的AccessKey'
        self.secret_key = "您的SecretKey"
        self.end_point = '"您的Endpoint'  # http://oss-cnbj01.cdsgss.com  http://oss-fra.cdsgss.com  ...
        self.client_type = 's3'
        self.s3_client = self.connection()

    # 初始化连接
    def connection(self):
        session = Session(self.access_key, self.secret_key)
        s3_client = session.client(self.client_type, endpoint_url=self.end_point)
        return s3_client

    """
    当前只支持默认私有桶创建
    """

    # 新建Bucket
    def create_bucket(self, bucketName):
        """
        :param bucketName: 桶名
        :param ACL: bucket的读写权限类型 备选值(ACLP ACLPR ACLPRW ACLAR)
        :return: Bool
        """
        self.s3_client.create_bucket(Bucket=bucketName)
        return self.add_bucket_ret(bucketName)

    # 删除Bucket
    def delete_bucket(self, bucketName):
        """
        :introduce: 删除指定的桶。只有该桶中所有的对象被删除了，该桶才能被删除。另外，只有该桶的拥有者才能删除它，与桶的访问控制权限无关
        :param bucketName: 要删除的桶名
        :return: Bool
        """
        try:
            self.s3_client.delete_bucket(Bucket=bucketName)
        except:
            return bucketName not in self.select_buckets()
        return self.del_bucket_ret(bucketName)

    # 判断删除桶是否成功
    def del_bucket_ret(self, bucketName):
        """
        :param bucketName: 删除指定的桶
        :return: Bool
        """
        try:
            waiter = self.s3_client.get_waiter('bucket_not_exists')
            waiter.wait(Bucket=bucketName, WaiterConfig={'Delay': 2, 'MaxAttempts': 20})
            return True
        except:
            return bucketName not in self.select_buckets()

    # 判断添加桶是否成功
    def add_bucket_ret(self, bucketName):
        """
        :param bucketName: 指定查询的桶名
        :return: Bool
        """
        try:
            waiter = self.s3_client.get_waiter('bucket_exists')
            waiter.wait(Bucket=bucketName, WaiterConfig={'Delay': 2, 'MaxAttempts': 20})
            return True
        except:
            return bucketName not in self.select_buckets()

    # 查看所有Bucket属性信息
    def select_buckets(self):
        """
        :return: bucket列表
        """
        resp = self.s3_client.list_buckets()['Buckets']
        return [data['Name'] for data in resp]

    # 查看Bucket访问权限
    def get_bucket_acl(self, bucketName):
        """
        :param bucketName: 要查询访问权限的桶
        :return:
        """
        resp = self.s3_client.get_bucket_acl(Bucket=bucketName)
        return resp

    """
    暂时不支持 权限修改
    """

    # 设置Bucket访问权限
    def set_bucket_acl(self, bucketName, ACl):
        """
        :param bucketName: 要设置访问权限的桶
        :param ACl: bucket的读写权限类型  可选值(ACLP ACLPR ACLPRW ACLAR)
        :return:
        """
        resp = self.s3_client.put_bucket_acl(Bucket=bucketName, ACL=ACl)
        return resp

    # 上传文件 (注：不开启版本控制功能，同目录同文件名会覆盖式更新, 开启后将以不同版本形式存在)
    def upload_file(self, bucketName, localFilePath, OssFilePath):
        """
        :param bucketName: 您的已经存在且配额可用的bucket名
        :param localFilePath: 需要上传的本地文件路径
        :param OssFilePath: 您上传文件后对象存储服务器端处存储的路径
        :return: Bool
        """
        with open(localFilePath, 'rb+') as f:
            try:
                resp = self.s3_client.put_object(Bucket=bucketName, Key=OssFilePath, Body=f.read())
                ret = self.exist_object_add(bucketName, OssFilePath, resp.get('VersionId', ''))
            except ClientError as err:
                print(err.response['Error'])
                ret = False
        return ret

    # 获取对象版本 (注：1.必须开启多版本控制功能  2.名称匹配不是完全匹配)
    def list_object_versions(self, bucketName, Prefix, Delimiter='/'):
        """
        :param bucketName: 您的已经存在且配额可用的bucket名
        :param Prefix: 只列举前缀为”img-“的所有文件 例：Prefix= 'img-'
        :param Delimiter: 目录分隔符
        :return: 返回对象版本列表
        """
        resp = self.s3_client.list_object_versions(Bucket=bucketName, Prefix=Prefix, Delimiter=Delimiter)
        return [{'VersionId': data['VersionId'], 'Key': data['Key']} for data in resp.get('Versions', list())]


    # 大文件分片上传 (注：同目录同文件名会覆盖式更新)
    def upload_largeFile(self, bucketName, localFilePath, OssFilePath, rangeSize=30 * 1024 * 1024, isMultiThread=False):
        """
        :param bucketName:  您的已经存在且配额可用的bucket名
        :param localFilePath: 本地文件
        :param OssFilePath: 您上传文件后对象存储服务器端处存储的路径
        :param rangeSize: 分片容量 单位/M
        :return:
        """
        s3 = boto3.resource('s3', endpoint_url=self.end_point, aws_access_key_id=self.access_key,
                            aws_secret_access_key=self.secret_key)
        bucket = s3.Bucket(bucketName)
        mpu = bucket.Object(OssFilePath).initiate_multipart_upload()  # step1. 初始化
        Parts = list()
        taskArray = list()
        i = 1
        with open(localFilePath, 'rb') as file:
            while True:
                data = file.read(rangeSize)  # 每个分块10MiB大小，可调整
                if data == b'':
                    break
                if isMultiThread:  # 可自行选择多线程或单线程
                    taskArray.append(Thread(target=self.actionThread, args=(i, Parts, mpu, data)))
                else:
                    self.actionThread(i, Parts, mpu, data)
                i += 1
        for one in taskArray:
            one.start()
        for one in taskArray:
            one.join()
        if len(Parts) != i - 1:
            return False
        else:
            Parts.sort(key=lambda x: x['PartNumber'])
        try:
            mpu.complete(MultipartUpload={'Parts': Parts})  # step3.完成上传
            return True
        except ClientError as err:
            print(err.response['Error'])
            return False

    def actionThread(self, tag, array, mpu, data):
        try:
            part = mpu.Part(tag)
            response = part.upload(Body=data)  # step2.上传分片
        except:
            return False
        array.append({
            'PartNumber': tag,
            'ETag': response['ETag']
        })
        print(str(tag) + '完成！')

    # 下载文件 整体下载
    def download(self, bucketName, OssFilePath, localFilePath, VersionId=''):
        """
        :param bucketName: 您的已经存在且配额可用的bucket名
        :param OssFilePath: 您上传文件后对象存储服务器端处存储的路径 (您该bucket中的对象名)
        :param localFilePath: 下载到本地文件路径 './test.bin'
        :param VersionId: 对应的版本号 （默认不传获取最新版本对象）
        :return: Bool
        """
        try:
            resp = self.s3_client.get_object(Bucket=bucketName, Key=OssFilePath, VersionId=VersionId)
            with open(localFilePath, 'wb') as f:
                f.write(resp['Body'].read())
            return True
        except:
            return False

    """
    暂时不支持 分片下载
    """

    # 下载文件 分片下载
    def download_range(self, bucketName, OssFilePath, localFilePath, rangeByte):
        """
        :param bucketName: 您的已经存在且配额可用的bucket名
        :param OssFilePath: 您上传文件后对象存储服务器端处存储的路径 (您该bucket中的对象名)
        :param localFilePath: 下载到本地文件路径 './test.bin'
        :param rangeByte: 文件字节范围    例：rangeByte= 'bytes=0‐10'
        :return: Bool
        """
        try:
            resp = self.s3_client.get_object(Bucket=bucketName, Key=OssFilePath, Range=rangeByte)
            with open(localFilePath, 'ab') as f:
                f.write(resp['Body'].read())
            print(resp)
            return True
        except:
            return False

    '''
    数据删除，开启版本控制后存在问题
    '''
    # 删除桶下的对象
    def delete_object(self, bucketName, OssFilePath, VersionId=''):
        """
        :care: 如果桶开启了多版本，s3_client.get_object需要参数 VersionId='对应的版本号'
        :param bucketName: 您的已经存在且配额可用的bucket名
        :param OssFilePath: 您上传文件后对象存储服务器端处存储的路径 (对象名)
        :param VersionId: 对应的版本号 （默认不传获取最新版本对象）
        :return: Bool
        """
        try:
            self.s3_client.delete_object(Bucket=bucketName, Key=OssFilePath, VersionId=VersionId)
            return self.exist_object_del(bucketName, OssFilePath, VersionId)
        except:
            return False

    # 批量删除名称匹配的同级目录下文件(未开启版本功能即删除唯一对象，开启则删除最新版本对象) （注： 若返回失败列表为空，但数据依旧存在，请检查文件路径参数）
    def delete_objects(self, bucketName, Prefix):
        """
        :param bucketName: 您的已经存在且配额可用的bucket名
        :param Prefix: 批量删除bucket1下的以2017-05为前缀的对象  例：Prefix= '2017‐05'
        :return: 返回删除失败文件名列表
        """
        try:
            s3 = boto3.resource(self.client_type, endpoint_url=self.end_point, aws_access_key_id=self.access_key,
                                aws_secret_access_key=self.secret_key)
            bucket = s3.Bucket(bucketName)
            objects_to_delete = []
            for obj in bucket.objects.filter(Prefix=Prefix, Delimiter='/'):
                objects_to_delete.append({'Key': obj.key})
            bucket.delete_objects(Delete={'Objects': objects_to_delete, 'Quiet': True})
        except ClientError as err:
            print(err)
            pass
        return self.like_list_object_files(bucketName, Prefix)

    # 批量删除名称匹配且版本不同的同级目录下文件 （注： 若返回失败列表为空，但数据依旧存在，请检查文件路径参数）
    def delete_objects_focusOn_VersionId(self, bucketName, Prefix):
        """
        :param bucketName: 您的已经存在且配额可用的bucket名
        :param Prefix: 批量删除bucket1下的以2017-05为前缀的对象  例：Prefix= '2017‐05'
        :return: 返回删除失败文件名列表
        """
        try:
            s3 = boto3.resource(self.client_type, endpoint_url=self.end_point, aws_access_key_id=self.access_key,
                                aws_secret_access_key=self.secret_key)
            bucket = s3.Bucket(bucketName)
            objects_to_delete = self.list_object_versions(bucketName, Prefix)
            bucket.delete_objects(Delete={'Objects': objects_to_delete, 'Quiet': True})
        except ClientError as err:
            print(err)
            pass
        return self.like_list_object_files(bucketName, Prefix)

    # 判断文件添加是否成功
    def exist_object_add(self, bucketName, OssFilePath, VersionId=''):
        """
        :param bucketName: 您的已经存在且配额可用的bucket名
        :param OssFilePath: 您上传文件后对象存储服务器端处存储的路径 (对象名)
        :param VersionId: 对应的版本号 （默认不传获取最新版本对象）
        :return: Bool
        """
        try:
            waiter = self.s3_client.get_waiter('object_exists')
            waiter.wait(Bucket=bucketName, Key=OssFilePath, VersionId=VersionId,
                        WaiterConfig={'Delay': 2, 'MaxAttempts': 20})
            return True
        except ClientError as err:
            print(err.response['Error']['Message'])
            return False

    # 判断文件删除是否成功
    def exist_object_del(self, bucketName, OssFilePath, VersionId=''):
        """
        :param bucketName: 您的已经存在且配额可用的bucket名
        :param OssFilePath: 您上传文件后对象存储服务器端处存储的路径 (对象名)
        :param VersionId: 对应的版本号 （默认不传获取最新版本对象）
        :return: Bool
        """
        try:
            waiter = self.s3_client.get_waiter('object_not_exists')
            waiter.wait(Bucket=bucketName, Key=OssFilePath, VersionId=VersionId,
                        WaiterConfig={'Delay': 2, 'MaxAttempts': 20})
            return True
        except ClientError as err:
            print(err.response['Error']['Message'])
            return False

    # Bucket内部文件拷贝 (注：同目录同文件名会覆盖式更新)
    def copy_object(self, NewBucketName, OldBucketName, NewFile, CopySource):
        """
        :param NewBucketName: 将要存放复制文件的bucket名
        :param OldBucketName: 原数据所在Bucket名
        :param NewFile: 新命名文件 例：copy.bin ; /log/copy.bin
        :param CopySource: 原数据文件 例：base.bin ; /log/base.bin
        :return: Bool
        """
        if '/' not in CopySource or CopySource[0] != '/':
            CopySource = '/' + CopySource
        try:
            resp = self.s3_client.copy_object(Bucket=NewBucketName, Key=NewFile,
                                       CopySource=str(OldBucketName + CopySource))
            if resp.get('CopySourceVersionId', '') or resp['CopyObjectResult']['ETag'] != '""':
                return True
            else:
                return False
        except CopySource as err:
            print(err.response['Error'])
            return False
        except Exception as err:
            print(err.message)
            return False


    # 列举Bucket目录下所有文件列表 (注：包括子级目录文件)
    def list_object_allfiles(self, bucketName):
        """
        :param bucketName: 您的已经存在且配额可用的bucket名
        :return: 目录下文件列表
        """
        resp = self.s3_client.list_objects(Bucket=bucketName)
        return [obj['Key'] for obj in resp.get('Contents', list())]

    # 按前缀罗列Bucket目录下所有文件列表  （注：不递归）
    def like_list_object_files(self, bucketName, Prefix):
        """
        :param bucketName: 您的已经存在且配额可用的bucket名
        :param Prefix: 只列举前缀为”img-“的所有文件 例：Prefix= 'img-'
        :return: 目录下文件列表
        """
        resp = self.s3_client.list_objects(Bucket=bucketName, Prefix=Prefix)
        return [obj['Key'] for obj in resp.get('Contents', list())]

    # 列举Bucket目录下所有一级目录列表   （注：不递归）
    def list_object_dirs(self, bucketName, Delimiter='/'):
        """
        :param bucketName: 您的已经存在且配额可用的bucket名
        :param Delimiter: 目录分隔符
        :return: 一级子目录列表
        """
        resp = self.s3_client.list_objects(Bucket=bucketName, Delimiter=Delimiter)
        return [obj['Prefix'] for obj in resp.get('CommonPrefixes', list())]

    # 列举Bucket目录下所有一级文件列表   （注：不递归）
    def list_object_files(self, bucketName, Delimiter='/'):
        """
        :param bucketName: 您的已经存在且配额可用的bucket名
        :param Delimiter: 目录分隔符
        :return: 一级目录文件列表
        """
        resp = self.s3_client.list_objects(Bucket=bucketName, Delimiter=Delimiter)
        return [obj['Key'] for obj in resp.get('Contents', list())]

    # 列举Bucket目录下指定子级目录下的一级目录列表 （注：不递归）
    def list_pdir_dirs(self, bucketName, Prefix, Delimiter='/'):
        """
        :param bucketName: 您的已经存在且配额可用的bucket名
        :param Prefix: 例 /log 获取log目录下的目录列表； /imgs/one 获取imgs/one目录下的目录列表
        :param Delimiter: 目录分隔符
        :return: 子目录列表
        """
        if '/' not in Prefix or Prefix[-1] != '/':
            Prefix += '/'
        resp = self.s3_client.list_objects(Bucket=bucketName, Delimiter=Delimiter, Prefix=Prefix)
        return [obj['Prefix'] for obj in resp.get('CommonPrefixes', list())]

    # 列举Bucket目录下指定子级目录下的一级文件列表 （注：不递归）
    def list_pdir_files(self, bucketName, Prefix, Delimiter='/'):
        """
        :param bucketName: 您的已经存在且配额可用的bucket名
        :param Prefix: 例 /log 获取log目录下的目录列表； /imgs 获取imgs目录下的目录列表
        :param Delimiter: 目录分隔符
        :return: 子目录文件列表
        """
        if '/' not in Prefix or Prefix[-1] != '/':
            Prefix += '/'
        resp = self.s3_client.list_objects(Bucket=bucketName, Delimiter=Delimiter, Prefix=Prefix)
        return [obj['Key'] for obj in resp.get('Contents', list())]

    # 查看文件访问权限
    def get_object_acl(self, bucketName, OssFilePath):
        """
        :param bucketName: 您的已经存在且配额可用的bucket名
        :param OssFilePath: 您上传文件后对象存储服务器端处存储的路径 (对象名)
        :return: 文件的读写权限和用户名
        """
        try:
            resp = self.s3_client.get_object_acl(Bucket=bucketName, Key=OssFilePath)
            return resp['Grants'], resp['Owner']
        except ClientError as err:
            print(err.response['Error'])
            return None, None
        except Exception as err:
            print(err.message)
            return None, None

    '''
    暂时不支持 文件权限设定
    '''

    # 设置文件访问权限
    def put_object_acl(self, bucketName, OssFilePath, ACL):
        """
        :param bucketName: 您的已经存在且配额可用的bucket名
        :param OssFilePath: 您上传文件后对象存储服务器端处存储的路径 (对象名)
        :param ACL: bucket的读写权限类型 备选值(ACLP ACLPR ACLPRW ACLAR)
        :return: Bool
        """
        resp = self.s3_client.put_object_acl(Bucket=bucketName, Key=OssFilePath, ACL=ACL)
        return resp

    # 生成私有下载链接
    def generate_presigned_url(self, bucketName, OssFilePath, ExpiresIn=3600, HttpMethod='GET', VersionId=''):
        """
        :param bucketName: 您的已经存在且配额可用的bucket名
        :param OssFilePath: 您上传文件后对象存储服务器端处存储的路径 (您的bucket下的要生成私有下载链接的对象)
        :param ExpiresIn: 过期时间 单位/S
        :param HttpMethod: 请求方式
        :return: 文件链接地址
        """
        resp = self.s3_client.generate_presigned_url(ClientMethod='get_object',
                                                     Params={'Bucket': bucketName, 'Key': OssFilePath, 'VersionId': VersionId},
                                                     ExpiresIn=ExpiresIn, HttpMethod=HttpMethod)
        return resp
