# coding=utf-8
import demo_python

action = demo_python.ActionDemo()

bucket = 'demo-test'

# 桶列表数据
# ret = action.create_bucket('demo-test1')

# ret = action.select_buckets()

# ret = action.delete_bucket('demo-test1')
#
# ret = action.set_bucket_acl('demo-test')
# 桶权限
# ret = action.get_bucket_acl('demo-test')

# ret = action.upload_file('demo-test', './test.py', 'mm3.py')

# ret = action.upload_largeFile(bucket, './worker.pdf', 'log/worker.pdf', isMultiThread=False)

# ret = action.exist_object('demo-test', '.main.py')

# ret = action.delete_object('demo-test', 'main.py')

# ret = action.like_list_object_files(bucket, 'm')

# ret = action.list_object_files('demo-test')

# ret = action.list_object_allfiles(bucket)

# ret = action.list_object_dirs(bucket)

# ret = action.list_pdir_dirs(bucket, 'log')

# ret = action.list_pdir_files(bucket, 'log/')

# ret = action.delete_objects(bucket, 'mm')

# ret = action.copy_object('demo-test', bucket, 'log/mainll.py', 'main.py')

# ret = action.get_object_acl(bucket, 'main.py')

# ret = action.put_object_acl(bucket, 'main.pt', OSSDemo.ACLPRW)

# ret = action.generate_presigned_url(bucket, 'main.py', 3600)

# ret = action.download(bucket, 'main.py', './tt.bin', '1616641869.77368')

# ret = action.list_object_versions(bucket, 'm')

# ret = action.delete_objects_focusOn_VersionId(bucket, 'm')
# print ret