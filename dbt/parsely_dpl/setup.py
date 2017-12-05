

import datetime
import argparse
import dbt
import parsely_raw_data.redshift as redshift

now = datetime.datetime.now()
today = now.date()
#should be updated to the start_date parameter that is passed through; hard coded for now
startdate = datetime.datetime.strptime('20171201', "%Y%m%d").date()


def daterange(d1, d2):
    return (d1 + datetime.timedelta(days=i) for i in range((d2 - d1).days + 1))


def main():
    parser = argparse.ArgumentParser(description='Run the historical back population of Parse.ly DPL data')
    parser.add_argument('target_table', nargs='?', default=None,
                        help='The target table for temporarily storing incremental data. This will be truncated after each load')
    parser.add_argument('redshift_host', nargs='?', default=None,
                        help='The Redshift host found in AWS.')
    parser.add_argument('redshift_user', nargs='?', default=None,
                        help='The Redshift user used to create tables and migrate data from S3 to Redshift')
    parser.add_argument('redshift_password', nargs='?', default=None,
                        help='The Redshift password for the user')
    parser.add_argument('redshift_database', nargs='?', default=None,
                        help='The Redshift database')
    parser.add_argument('redshift_port', nargs='?', default=None,
                        help='The Redshift port')
    parser.add_argument('keep_extra_data', nargs='?', default='False',
                        help='Keep extra data field, default is Yes.')
    parser.add_argument('dpl_network', nargs='?', default=None,
                        help='The network name of dpl data in S3')
    parser.add_argument('start_date', nargs='?', default=None,
                        help='The first day to process data from S3 to Redshift')
    args = parser.parse_args()
    #names = generate_names(letter=args.letter)

    redshift.create_table(table_name=args.target_table,host=args.redshift_host, user=args.redshift_user, password=args.redshift_password, database=args.redshift_database,port=args.redshift_port,keep_extra_data=args.keep_extra_data)

    for d in daterange(startdate, today):
        prefix = 'events/'+ d.strftime('%Y/%m/%d')
        print prefix
        #run copy_from_s3
        redshift.copy_from_s3(network=args.dpl_network, s3_prefix=prefix,table_name=args.target_table,host=args.redshift_host,user=args.redshift_user,password=args.redshift_password,database=args.redshift_database,port=args.redshift_port,aws_access_key_id="accesskey",aws_secret_access_key="secret")

        #run dbt command
        #./dbt run --models base.*+
        dbt.main.main(models="base.*+")

if __name__ == "__main__":
    main()
