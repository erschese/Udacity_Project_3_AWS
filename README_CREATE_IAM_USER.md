#IAM Management Console

##Create an IAM User

Here, you'll create an IAM user that you will use to access your Redshift cluster.

    -Sign in to the AWS Management Console and open the IAM console at https://console.aws.amazon.com/iam/.
    -In the left navigation pane, choose Users.
    -Choose Add User.
    -Enter a name for your user (e.g. airflow_redshift_user)
    -Choose Programmatic access, then choose Next: Permissions.
    -Choose Attach existing policies directly.
    -Search for redshift and select "AmazonRedshiftFullAccess". 
     Then, search for S3 and select "AmazonS3ReadOnlyAccess". 
     Then, search for "AdministratorAccess".
     After selecting both policies, choose Next: Tags.
    -Skip this page and choose Next: Review.
    -Review your choices and choose Create user.
    -Save your credentials! This is the only time you can view or download these credentials on AWS. Choose Download .csv to download these credentials and then save this file to a safe location. You'll need to copy and paste this Access key ID and Secret access key in the next step.

We strongly advise you to keep this Access key ID and Secret access key closely guarded, including not putting them in a GitHub public repo, etc.

##Create an IAM Role

Here, you'll create an IAM role that you will later attach to your Redshift cluster to enable your cluster to load data from Amazon S3 buckets. Read more about IAM roles and Redshift here.

    -Sign in to the AWS Management Console and open the IAM console at https://console.aws.amazon.com/iam/.
    -In the left navigation pane, choose Roles.
    -Choose Create role.
    -In the AWS Service group, choose Redshift.
    -Under Select your use case, choose Redshift - Customizable, and then Next: Permissions.
    -On the Attach permissions policies page, choose AmazonS3ReadOnlyAccess, and then choose Next: Tags.
    -Skip this page and choose Next: Review.
    -For Role name, enter myRedshiftRole, and then choose Create Role.

##Create Security Group
Here, you'll create a security group you will later use to authorize access to your Redshift cluster.

    -Go to your Amazon EC2 console and under Network and Security in the left navigation pane, select Security Groups.
    -Choose the Create Security Group button.
    -Enter redshift_security_group for Security group name.
    -Enter "authorize redshift cluster access" for Description.
    -Select the Inbound tab under Security group rules.
    -Click on Add Rule and enter the following values:

        Type: Custom TCP Rule.
        Protocol: TCP.
        Port Range: 5439. The default port for Amazon Redshift is 5439, but your port might be different. See note on determining your firewall rules on the earlier "AWS Setup Instructions" page in this lesson.
        Source: select Custom IP, then type 0.0.0.0/0.
        
    Important: Using 0.0.0.0/0 is not recommended for anything 
    other than demonstration purposes because it allows access 
    from any computer on the internet. In a real environment, 
    you would create inbound rules based on your own network 
    settings.
    
    -Choose Create.