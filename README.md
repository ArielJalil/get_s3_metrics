# Get AWS S3 Bucket metrics (Size and object count)

CLI Command to get AWS S3 bucket metrics

## Script usage

```bash
❯ python s3_bucket_metrics.py --help
Usage: s3_bucket_metrics.py [OPTIONS]

  Query S3 bucket metrics.

Options:
  -p, --profile TEXT  AWS cli profile set in ~/.aws/config file.
  -b, --bucket TEXT   Select a specific S3 bucket, if it is not set all
                      buckets in the AWS account are queried.
  --help              Show this message and exit.
  ```

## Author and Lincense

This script has been written by [Ariel Jall](https://github.com/ArielJalil) and it is released under
 [GNU 3.0](https://www.gnu.org/licenses/gpl-3.0.en.html).
