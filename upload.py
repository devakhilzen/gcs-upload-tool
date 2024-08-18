import argparse
from google.cloud import storage
import os
from tqdm import tqdm

def upload_to_gcs(bucket_name, source_file_name, destination_blob_name):
    try:
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(destination_blob_name)

        file_size = os.path.getsize(source_file_name)
        chunk_size = 256 * 1024  # Set the chunk size to 256 KB
        pbar = tqdm(total=file_size, unit='B', unit_scale=True, desc=destination_blob_name)

        try:
            with open(source_file_name, 'rb') as file_obj:
                while True:
                    chunk = file_obj.read(chunk_size)
                    if not chunk:
                        break
                    blob.upload_from_string(chunk, content_type='application/octet-stream', if_generation_match=0)
                    pbar.update(len(chunk))
        except FileNotFoundError:
            print(f"Error: The file {source_file_name} does not exist.")
            return
        except Exception as e:
            print(f"An error occurred while uploading the file: {e}")
            return
        finally:
            pbar.close()

        print(f"File {source_file_name} uploaded Successfully.")
    except Exception as e:
        print(f"An error occurred: {e}")

def main():
    parser = argparse.ArgumentParser(description='Upload files to Google Cloud Storage.')
    parser.add_argument('file_path', help='The path to the file you want to upload.')
    parser.add_argument('bucket_name', help='The name of the GCS bucket.')
    parser.add_argument('destination_blob_name', help='The destination blob name in GCS.')

    args = parser.parse_args()

    if not os.path.isfile(args.file_path):
        print(f"Error: The file {args.file_path} does not exist.")
        return

    upload_to_gcs(args.bucket_name, args.file_path, args.destination_blob_name)

if __name__ == '__main__':
    main()

