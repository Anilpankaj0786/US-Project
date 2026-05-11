# import boto3
# from botocore.exceptions import ClientError

# #  AWS region (Mumbai - India)
# REGION = "ap-south-1"

# #  Function: Bucket check kare aur agar na ho toh create kare

# def create_bucket_if_not_exists(bucket_name):

#     #  S3 client bana rahe hain specific region ke saath
#     s3_client = boto3.client("s3", region_name=REGION)

#     try:
#         #  Check kar rahe hain bucket exist karti hai ya nahi
#         s3_client.head_bucket(Bucket=bucket_name)

#         print(f" Bucket '{bucket_name}' already exist karti hai")

#     except ClientError as error:
#         #  Error ka matlab ho sakta hai:
#         #    1. Bucket exist nahi karti
#         #    2. Ya access denied hai

#         print(f" Bucket '{bucket_name}' nahi mili, create karne ki koshish...")

#         try:
#             #  Nayi bucket create kar rahe hain
#             s3_client.create_bucket(
#                 Bucket=bucket_name,
#                 CreateBucketConfiguration={
#                     "LocationConstraint": REGION
#                 }
#             )

#             print(f" Bucket '{bucket_name}' successfully create ho gayi")

#         except ClientError as create_error:
#             print(f" Bucket create nahi ho paayi: {create_error}")


# # Function: Local file ko S3 bucket me upload karna

# def upload_file_to_s3(file_path, bucket_name, s3_key):

#     #  Default S3 client (region optional hai upload ke liye)
#     s3_client = boto3.client("s3")

#     print(f" Upload start → {s3_key}")

#     try:
#         #  File upload ho rahi hai S3 par
#         s3_client.upload_file(file_path, bucket_name, s3_key)

#         print(f" Upload complete → {s3_key}")

#     except ClientError as error:
#         print(f" Upload fail ho gaya → {s3_key}")
#         #  Optional: error detail dekh sakte ho
#         # print(error)


import boto3
from botocore.exceptions import ClientError

REGION = "ap-south-1"


def create_bucket_if_not_exists(bucket_name):

    s3_client = boto3.client("s3", region_name=REGION)

    try:
        s3_client.head_bucket(Bucket=bucket_name)
        print(f" Bucket '{bucket_name}' already exist karti hai")

    except ClientError:

        print(f" Bucket '{bucket_name}' nahi mili, create kar rahe hain...")

        try:
            s3_client.create_bucket(
                Bucket=bucket_name,
                CreateBucketConfiguration={"LocationConstraint": REGION}
            )

            print(f" Bucket '{bucket_name}' create ho gayi")

        except ClientError as e:
            print(f" Error: {e}")


def upload_file_to_s3(file_path, bucket_name, s3_key):

    s3_client = boto3.client("s3")

    print(f" Upload start → {s3_key}")

    try:
        s3_client.upload_file(file_path, bucket_name, s3_key)
        print(f" Upload complete → {s3_key}")

    except ClientError:
        print(f" Upload fail → {s3_key}")