from django.conf import settings
from django.core.exceptions import ImproperlyConfigured, SuspiciousOperation
from django.core.files import File
from django.core.files.storage import Storage
from django.utils.encoding import force_text

import boto3
import botocore


class S3Storage(Storage):
    def __init__(self, *args, **kwargs):
        super(S3Storage, self).__init__(*args, **kwargs)
        try:
            self.s3 = boto3.resource(
                's3',
                aws_access_key_id=settings.AWS_ACCESS_KEY_ID,
                aws_secret_access_key=settings.AWS_SECRET_ACCESS_KEY)
            self.bucket = self.s3.Bucket(settings.AWS_S3_BUCKET)
        except AttributeError:
            raise ImproperlyConfigured("Make sure your AWS config is complete")

    def accessed_time(self, name):
        raise NotImplementedError

    def created_time(self, name):
        raise NotImplementedError

    def delete(self, name):
        assert name, "The name argument is not allowed to be empty."
        self.bucket.Object(name).delete()

    def exists(self, name):
        try:
            self.s3.meta.client.head_object(Bucket=self.bucket.name, Key=name)
        except botocore.exceptions.ClientError as e:
            if e.response['Error']['Code'] == "404":
                return False
            raise e
        return True

    def get_available_name(self, name, max_length=None):
        if not self.exists(name):
            return name

        if '.' in name:
            name, ext = '.'.join(name.split('.')[:-1]), name.split('.')[-1]
        else:
            ext = ''

        for i in xrange(1, 10):
            ranged_name = '.'.join(['_'.join([name, str(i)]), ext])
            if not self.exists(ranged_name):
                return ranged_name
        raise SuspiciousOperation

    def get_valid_name(self, name):
        return name

    def listdir(self, path):
        directories = []
        files = []
        for obj in self.bucket.objects.filter(Prefix=path):
            if obj.key.endswith('/'):
                directories.append(obj.key)
            else:
                files.append(obj.key)
        return (directories, files)

    def modified_time(self, name):
        return self.bucket.Object(name).last_modified

    def open(self, name, mode='rb'):
        response = self.s3.get_object(Bucket=self.bucket.name, Key=name)
        return File(response['Body'].read(), name)

    def path(self, name):
        raise NotImplementedError

    def save(self, name, content, max_length=None):
        # Get the proper name for the file, as it will actually be saved.
        if name is None:
            name = content.name
        name = self.get_available_name(name, max_length)

        if hasattr(content, 'temporary_file_path'):
            self.bucket.upload_file(content.temporary_file_path(), name)
        else:
            self.bucket.put_object(Body=content.read(), Key=name)

        # Store filenames with forward slashes
        return force_text(name.replace('\\', '/'))

    def size(self, name):
        return self.bucket.Object(name).content_length

    def url(self, name):
        return 'https://s3-{0}.amazonaws.com/{1}/{2}'.format(
            self.s3.meta.client.get_bucket_location(Bucket=self.bucket.name),
            self.bucket.name, name)
