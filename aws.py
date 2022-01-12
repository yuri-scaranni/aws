#==============================================================================
#     Script : s3_aws_functions.py
#     Autor  : Yuri Scaranni
#     Data   : 30/10/2020
#
#   Objetivo : Funções utilizadas para conexão com AWS, upload & download de arquivos do S3,
#           criação de bucket, listagem de arquivos em bucket, listagem de buckets, mover arquivos de um
#           bucket para outro
#
#   Dependencia:
#              config/auth
#
#==============================================================================
import boto3
import os
from botocore.exceptions import NoCredentialsError
from boto3 import client
import time
import auth as a

ACCESS_KEY, SECRET_KEY = a.aws_credentials()


def aws_session(region_name='sa-east-1'):
    return boto3.session.Session(aws_access_key_id=ACCESS_KEY,
                                aws_secret_access_key=SECRET_KEY,
                                region_name=region_name)


def list_buckets():
    session = aws_session()
    s3_resource = session.resource('s3')
    return [bucket.name for bucket in s3_resource.buckets.all()]


def list_s3_keys(bucket_name):
    files_in_bucket = []
    session = aws_session()
    conn = session.client('s3')
    try:
        for key in conn.list_objects(Bucket=bucket_name)['Contents']:
            files_in_bucket.append(key['Key'])
    except Exception as e:
        print('Bucket Vazio')
    finally:
        return files_in_bucket


def make_bucket(name):
    session = aws_session()
    s3_resource = session.resource('s3')
    if name in list_buckets():
        return print('>>>> Bucket encontrado')
    else:
        print(f'>>>> Bucket não encontrado, criando bucket {name}...')
        return s3_resource.create_bucket(Bucket=name, CreateBucketConfiguration={'LocationConstraint': 'sa-east-1'})


def upload_file_to_bucket(bucket_name, file_path):
    session = aws_session()
    s3_resource = session.resource('s3')
    file_dir, file_name = os.path.split(file_path)

    bucket = s3_resource.Bucket(bucket_name)
    bucket.upload_file(
      Filename=file_path,
      Key=file_name,
      ExtraArgs={'ACL': 'public-read'}
    )
    print('>>>> Upload completo')

    s3_url = f"https://{bucket_name}.s3.amazonaws.com/{file_name}"
    return s3_url


def download_file_from_bucket(bucket_name, file_from_s3, name_download_exten_file):
    session = aws_session()
    s3_resource = session.resource('s3')
    bucket = s3_resource.Bucket(bucket_name)
    bucket.download_file(Key=file_from_s3, Filename=name_download_exten_file)
    return name_download_exten_file


def copy_file_to_another_bucket(origin_bucket_name, destiny_bucket_name, file_name_origin):
    session = aws_session()
    s3_resource = session.resource('s3')
    copy_source = {
        'Bucket': origin_bucket_name,
        'Key': file_name_origin
    }

    make_bucket(destiny_bucket_name)

    f_name, f_extension = os.path.splitext(file_name_origin)
    file_name_destiny = f_name + time.strftime('_%d_%m_%Y') + f_extension

    s3_resource.meta.client.copy(copy_source, destiny_bucket_name, file_name_destiny)
    print(f'>>>> Arquivo copiado de {origin_bucket_name} para {destiny_bucket_name}')

    s3_resource.Object(origin_bucket_name, file_name_origin).delete()
    print(f'>>>> {file_name_origin} deletado de {origin_bucket_name}')
