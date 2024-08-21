# -*- coding: utf-8 -*-
"""
Query S3 Buckets and fetch CloudWatch metrics:
- Bucket size
- Bucket object count

Usage: s3_bucket_metrics.py [OPTIONS]

  Query S3 bucket metrics.

Options:
  -p, --profile TEXT  AWS cli profile set in ~/.aws/config file.
  -b, --bucket TEXT   Select a specific S3 bucket, if this is not set all
                      buckets in the AWS account are queried.
  --help              Show this message and exit.

"""

from logging import getLogger
from datetime import datetime, timedelta
import os
import sys
import click
import animation
from prettytable import PrettyTable
from boto3 import Session
from botocore.credentials import JSONFileCache
from botocore.exceptions import ProfileNotFound

LOGGER = getLogger(__name__)


class AwsSession:  # pylint: disable=R0903
    """Manage boto3 session."""

    def __init__(self, profile: str, region='ap-southeast-2', authentication='sso') -> None:
        """Initialize class."""
        self._instance_logger = LOGGER.getChild(str(id(self)))
        self.profile = profile
        self.region = region
        if authentication == 'sso' or authentication == 'cli':  # pylint: disable=R1714
            self.authentication = authentication
        else:
            self._instance_logger.error(
                'Allowed values for authentication variable are sso or cli.'
            )
            sys.exit(-1)

    def cli(self):
        """Start a session to be used from CLI."""
        cache = f".aws/{self.authentication}/cache"

        cli_cache = os.path.join(os.path.expanduser('~'), cache)
        cli_session = None
        try:
            cli_session = Session(
                profile_name=self.profile,
                region_name=self.region
            )

        except ProfileNotFound as e:
            self._instance_logger(e)
            sys.exit(-1)

        cli_session._session.get_component(  # pylint: disable=W0212
            'credential_provider'
        ).get_provider(
            'assume-role'
        ).cache = JSONFileCache(
            cli_cache
        )

        return cli_session


class CwMetric:
    """Manage CloudWatch Metric."""

    def __init__(self, client: object) -> None:
        """Set class variables."""
        self.client = client

    def get_s3_metric(self, bucket: str, metric: str, stg_type: str, statistic='Sum') -> dict:
        """Query S3 Namespace Metric."""
        result = self.client.get_metric_statistics(
            Namespace="AWS/S3",
            Dimensions=[{"Name": "BucketName", "Value": bucket},
                        {"Name": "StorageType", "Value": stg_type}],
            MetricName=metric,
            StartTime=datetime.now() - timedelta(2),
            EndTime=datetime.now(),
            Period=86400,
            Statistics=[statistic],
        )
        try:
            return {
                'Value': result["Datapoints"][0][statistic],
                'Unit': result['Datapoints'][0]['Unit']
            }
        except:  # pylint: disable=W0702
            return {
                'Value': 0,
                'Unit': None
            }

    def get_bucket_size(self, bucket: str) -> dict:
        """Query S3 bucket Standard Storage usage."""
        return self.get_s3_metric(bucket, 'BucketSizeBytes', 'StandardStorage')

    def get_bucket_object_count(self, bucket: str) -> dict:
        """Query S3 bucket Objects count."""
        return self.get_s3_metric(bucket, 'NumberOfObjects', 'AllStorageTypes')

    def display_bucket_size(self, bucket: str) -> None:
        """Display S3 bucket Standard Storage usage."""
        size = self.get_bucket_size(bucket)
        print(f"{bucket}: {int(size['Value'])} {size['Unit']}")

    def display_object_count(self, bucket: str) -> None:
        """Display S3 bucket Standard Storage usage."""
        count = self.get_bucket_object_count(bucket)
        print(f"{bucket}: {int(count['Value'])} Object/s")


@click.command()
@click.option(
    '-p',
    '--profile',
    show_default=False,
    nargs=1,
    type=str,
    help='AWS cli profile set in ~/.aws/config file.'
)
@click.option(
    '-b',
    '--bucket',
    show_default=False,
    default=None,
    nargs=1,
    type=str,
    help='Select a specific S3 bucket, if it is not set all buckets in the AWS account are queried.'
)
@animation.wait('pulse', 'Processing your query', .5, 'green')
def query_bucket(profile: str, bucket: str) -> None:
    """Query S3 bucket metrics."""

    session = AwsSession(profile).cli()
    cw_client = session.client('cloudwatch')
    cw = CwMetric(cw_client)

    if bucket:
        buckets = [bucket]
    else:
        s3_client = session.client('s3')
        s3_buckets = s3_client.list_buckets()
        buckets = []
        for b in s3_buckets['Buckets']:
            buckets.append(b['Name'])

    bytes_sum = 0
    count_sum = 0
    pt = PrettyTable(('Bucket name', 'Size [Bytes]', 'Object count'))

    for b in buckets:
        pt.align['Bucket name'] = 'l'
        pt.align['Size [Bytes]'] = 'r'
        pt.align['Object count'] = 'r'
        pt.add_row(
            (
                b,
                cw.get_bucket_size(b)['Value'],
                cw.get_bucket_object_count(b)['Value']
            )
        )
        bytes_sum += int(cw.get_bucket_size(b)['Value'])
        count_sum += int(cw.get_bucket_object_count(b)['Value'])

    print("\n")
    print(pt.get_string(title='S3 bucket metrics'))
    print("\nGrand total:\n")
    print(f"Storage space used (GB).: {bytes_sum / 1024 / 1024 / 1024:.2f}")
    print(f"Objects count...........: {count_sum}")


if __name__ == '__main__':
    query_bucket()  # pylint: disable=E1120
