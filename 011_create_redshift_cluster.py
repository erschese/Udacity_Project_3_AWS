# This is a sample Python script
import myAWSKit

# Press the green button in the gutter to run the script
if __name__ == '__main__':

    # get parameters
    df_dwh_conf = myAWSKit.get_dwh_param_as_df('030_dwh.cfg')
    print('\ndwh configuration:\n', df_dwh_conf)

    # AWS - IAM
    # %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
    # set iam
    iam = myAWSKit.set_iam(df_dwh_conf[df_dwh_conf['Param'] == 'KEY'].values[0][1],
                           df_dwh_conf[df_dwh_conf['Param'] == 'SECRET'].values[0][1])

    # create iam role and attached policy
    roleArn = myAWSKit.create_iam_role_and_policy(iam,
                                                  df_dwh_conf[df_dwh_conf['Param'] == 'DB_IAM_ROLE_NAME'].values[0][1])

    # AWS - REDSHIFT
    # %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
    # set redshift
    redshift = myAWSKit.set_redshift(df_dwh_conf[df_dwh_conf['Param'] == 'KEY'].values[0][1],
                                     df_dwh_conf[df_dwh_conf['Param'] == 'SECRET'].values[0][1])

    # create redshift cluster
    myAWSKit.create_redshift_cluster(redshift, roleArn,
                                     df_dwh_conf[df_dwh_conf['Param'] == 'DB_CLUSTER_TYPE'].values[0][1],
                                     df_dwh_conf[df_dwh_conf['Param'] == 'DB_NODE_TYPE'].values[0][1],
                                     df_dwh_conf[df_dwh_conf['Param'] == 'DB_NUM_NODES'].values[0][1],
                                     df_dwh_conf[df_dwh_conf['Param'] == 'DB_NAME'].values[0][1],
                                     df_dwh_conf[df_dwh_conf['Param'] == 'DB_CLUSTER_IDENTIFIER'].values[0][1],
                                     df_dwh_conf[df_dwh_conf['Param'] == 'DB_USER'].values[0][1],
                                     df_dwh_conf[df_dwh_conf['Param'] == 'DB_PASSWORD'].values[0][1],)


# See PyCharm help at https://www.jetbrains.com/help/pycharm/
