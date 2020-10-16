import myAWSKit

# Press the green button in the gutter to run the script.
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

    # clean up project
    # %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
    # delete redshift cluster
    myAWSKit.delete_redshift_cluster(redshift, df_dwh_conf[df_dwh_conf['Param'] == 'DB_CLUSTER_IDENTIFIER']
                                     .values[0][1])

    # delete iam role and attached policy
    myAWSKit.delete_iam_role_and_policy(iam, df_dwh_conf[df_dwh_conf['Param'] == 'DB_IAM_ROLE_NAME'].values[0][1])