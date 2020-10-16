import configparser
import pandas as pd
import boto3
import json


def set_iam(key_, secret_):

    # initiate iam variable
    iam = boto3.client('iam', aws_access_key_id=key_,
                       aws_secret_access_key=secret_,
                       region_name='us-west-2'
                       )
    return iam


def set_redshift(key_, secret_):

    redshift = boto3.client('redshift',
                            region_name="us-west-2",
                            aws_access_key_id=key_,
                            aws_secret_access_key=secret_
                            )
    return redshift


def create_redshift_cluster(redshift_, role_arn_, cluster_type_, node_type_, number_of_node_, db_name_,
                            cluster_identifier_, db_user_name_, db_user_psw_):
    try:
        response = redshift_.create_cluster(
            # HW
            ClusterType=cluster_type_,
            NodeType=node_type_,
            NumberOfNodes=int(number_of_node_),

            # Identifiers & Credentials
            DBName=db_name_,
            ClusterIdentifier=cluster_identifier_,
            MasterUsername=db_user_name_,
            MasterUserPassword=db_user_psw_,

            # Roles (for s3 access)
            IamRoles=[role_arn_]
        )
    except Exception as e:
        print(e)

def delete_redshift_cluster(redshift_, cluster_identifier_):

    #### CAREFUL!!
    redshift_.delete_cluster(ClusterIdentifier=cluster_identifier_, SkipFinalClusterSnapshot=True)
    #### CAREFUL!!

def get_redshift_props_as_df(props):

    pd.set_option('display.max_colwidth', None)
    keysToShow = ["ClusterIdentifier", "NodeType", "ClusterStatus", "MasterUsername", "DBName", "Endpoint", "NumberOfNodes", 'VpcId']
    x = [(k, v) for k,v in props.items() if k in keysToShow]
    return pd.DataFrame(data=x, columns=["Key", "Value"])


def create_iam_role_and_policy(iam_, role_name_):

    # create new role
    try:
        print('\n1.1 Creating a new IAM Role')
        dwhRole = iam_.create_role(
            Path='/',
            RoleName=role_name_,
            Description="Allows Redshift clusters to call AWS services on your behalf.",
            AssumeRolePolicyDocument=json.dumps(
                {'Statement': [{'Action': 'sts:AssumeRole',
                                'Effect': 'Allow',
                                'Principal': {'Service': 'redshift.amazonaws.com'}}],
                 'Version': '2012-10-17'})
        )
    except Exception as e:
        print(e)

    # create new policy
    print("1.2 Attaching Policy")

    iam_.attach_role_policy(RoleName=role_name_,
                           PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess"
                           )['ResponseMetadata']['HTTPStatusCode']

    print("1.3 Get the IAM role ARN")
    roleArn = iam_.get_role(RoleName=role_name_)['Role']['Arn']

    print(roleArn)

    print("1.4 Attaching Policy")

    iam_.attach_role_policy(RoleName=role_name_,
                           PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess"
                           )['ResponseMetadata']['HTTPStatusCode']
    return roleArn


def get_arnrole(iam_, role_name_):
    return iam_.get_role(RoleName=role_name_)['Role']['Arn']


def delete_iam_role_and_policy(iam_, role_name_):

    #### CAREFUL!!
    iam_.detach_role_policy(RoleName=role_name_, PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess")
    iam_.delete_role(RoleName=role_name_)
    #### CAREFUL!!


def get_dwh_param_as_df(filepath_):

    # init parser to read database configurations from file
    config = configparser.ConfigParser()
    config.read_file(open(filepath_))

    # AWS credentials
    KEY = config.get('AWS', 'KEY')
    SECRET = config.get('AWS', 'SECRET')

    # S3 settings
    LOG_DATA = config.get('S3', 'LOG_DATA')
    LOG_JSONPATH = config.get('S3', 'LOG_JSONPATH')
    SONG_DATA = config.get('S3', 'SONG_DATA')

    # database cluster settings
    DB_CLUSTER_TYPE = config.get("CLUSTER", "DB_CLUSTER_TYPE")
    DB_NUM_NODES = config.get("CLUSTER", "DB_NUM_NODES")
    DB_NODE_TYPE = config.get("CLUSTER", "DB_NODE_TYPE")
    DB_IAM_ROLE_NAME = config.get("CLUSTER", "DB_IAM_ROLE_NAME")
    DB_CLUSTER_IDENTIFIER = config.get("CLUSTER", "DB_CLUSTER_IDENTIFIER")

    # database and database user settings
    DB_NAME = config.get("CLUSTER", "DB_NAME")
    DB_USER = config.get("CLUSTER", "DB_USER")
    DB_PASSWORD = config.get("CLUSTER", "DB_PASSWORD")
    DB_PORT = config.get("CLUSTER", "DB_PORT")

    # create data frame
    df = pd.DataFrame({
        "Param":
        ["KEY", "SECRET", "LOG_DATA", "LOG_JSONPATH", "SONG_DATA", "DB_CLUSTER_TYPE", "DB_NUM_NODES", "DB_NODE_TYPE",
         "DB_IAM_ROLE_NAME", "DB_CLUSTER_IDENTIFIER", "DB_NAME", "DB_USER", "DB_PASSWORD", "DB_PORT"],
        "Value":
        [KEY, SECRET, LOG_DATA, LOG_JSONPATH, SONG_DATA, DB_CLUSTER_TYPE, DB_NUM_NODES, DB_NODE_TYPE,
         DB_IAM_ROLE_NAME, DB_CLUSTER_IDENTIFIER, DB_NAME, DB_USER, DB_PASSWORD, DB_PORT]
             })

    # return data frame
    return df
