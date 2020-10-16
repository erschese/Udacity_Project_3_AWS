import myAWSKit

if __name__ == '__main__':

    # get parameters
    df_dwh_conf = myAWSKit.get_dwh_param_as_df('030_dwh.cfg')
    print('\ndwh configuration:\n', df_dwh_conf)

    # AWS - IAM
    # %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
    # set iam
    iam = myAWSKit.set_iam(df_dwh_conf[df_dwh_conf['Param'] == 'KEY'].values[0][1],
                           df_dwh_conf[df_dwh_conf['Param'] == 'SECRET'].values[0][1])

    # AWS - REDSHIFT
    # %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
    # set redshift
    redshift = myAWSKit.set_redshift(df_dwh_conf[df_dwh_conf['Param'] == 'KEY'].values[0][1],
                                     df_dwh_conf[df_dwh_conf['Param'] == 'SECRET'].values[0][1])

    # print IAM role arn -> write arn role into file 'dwh.cfg'
    # %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
    print("\n1.1 Get the IAM role ARN")
    print(myAWSKit.get_arnrole(iam, df_dwh_conf[df_dwh_conf['Param'] == 'DB_IAM_ROLE_NAME'].values[0][1]))

    # print redshift properties -> write host into file 'dwh.cfg'
    # %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
    print("1.2 Get redshift properties")
    # get redshift
    myClusterProps = redshift.describe_clusters(
        ClusterIdentifier=df_dwh_conf[df_dwh_conf['Param'] == 'DB_CLUSTER_IDENTIFIER'].values[0][1])['Clusters'][0]
    df_redshift_prop = myAWSKit.get_redshift_props_as_df(myClusterProps)
    print('\nredshift cluster properties:\n', df_redshift_prop)