import sys
from functions.transform_jobs_app_json_to_csv import transform_jobs_app_json_to_csv

if __name__ == "__main__":
    bucket_name = sys.argv[1]
    file_path = sys.argv[2]

    event = {
        "Records": [{
            "s3": {
                "bucket": {
                    "name": bucket_name
                },
                "object": {
                    "key": file_path
                }
            }
        }]
    }

    transform_jobs_json_to_csv(event, {})