
import boto3
from botocore.exceptions import ClientError
import json






secret_name = "redshift_credentials"
region_name = "eu-north-1"


session = boto3.session.Session()
client = session.client(
    service_name='secretsmanager',
    region_name=region_name
)

try:
    get_secret_value_response = client.get_secret_value(
        SecretId=secret_name
    )
except ClientError as e:
    raise e

secret = get_secret_value_response['SecretString']

secret_dict = json.loads(secret)


user_name = secret_dict['redshift_user']

password = secret_dict['redshift_password']

print_string = f"export DBT_REDSHIFT_USER={user_name}\n export DBT_REDSHIFT_PASSWORD={password}"


print(print_string)


