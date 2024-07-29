import csv
import io
import json
import os
import shlex
import subprocess
import threading
import time
import requests
import logging
from pathlib import Path

# Configuration constants
SLEEP_TIME = 20
OUTPUT_DIR = Path('output_chunks')
MAX_CHUNK_SIZE_MB = 100
LOG_FILE = 'sfUtility.log'
VERSION = 60.0

# Set up logging
logging.basicConfig(filename=LOG_FILE, level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


def ensure_output_dir_is_empty(output_dir=OUTPUT_DIR):
    """Ensure the output directory is empty before processing."""
    if output_dir.exists():
        for file in output_dir.iterdir():
            try:
                if file.is_file():
                    file.unlink()
                elif file.is_dir():
                    file.rmdir()
            except Exception as e:
                logging.error(f"Error deleting file {file}: {e}")


class SFUtility:
    def __init__(self, alias_name):
        self.access_token = None
        self.instance_url = None
        self.alias_name = alias_name
        self.get_access_token_from_alias()

    def run_sfdx_command(self, command):
        """Runs an SFDX command and returns the output."""
        command = f'{command} --target-org {self.alias_name} --json'
        try:
            command_list = shlex.split(command) if isinstance(command, str) else command
            if command_list[0] != 'sf':
                command_list.insert(0, 'sf')
            is_windows = os.name == 'nt'
            logging.info(f'Running command: {shlex.join(command_list)}')
            completed_process = subprocess.run(
                command_list,
                shell=is_windows,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True
            )
            return completed_process.stdout
        except Exception as e:
            logging.error(f"Error running command {e}")
            return None

    def get_access_token_from_alias(self):
        """Retrieves and stores the access token and instance URL for a given alias."""
        command = f"org display --verbose"
        output = self.run_sfdx_command(command)
        if output:
            try:
                org_info = json.loads(output)
                self.access_token = org_info['result']['accessToken']
                self.instance_url = org_info['result']['instanceUrl']
                logging.info("Access token and instance URL have been stored.")
            except Exception as e:
                logging.error(f"Failed to retrieve access token: {e}")
        else:
            logging.error(f"Failed to retrieve org information for alias {self.alias_name}.")

    def bulk_query(self, soql_query, output_file_path):
        start_time = time.time()
        command = f"sf data query --query \"{soql_query}\" --result-format csv --bulk"
        output = self.run_sfdx_command(command)
        if output:
            try:
                job_info = json.loads(output)
                bulk_query_id = job_info['result']['id']
                if self.is_bulk_query_done(bulk_query_id):
                    self.get_result_from_bulk_query(bulk_query_id, output_file_path)
                logging.info('Finished writing to csv')
            except Exception as e:
                logging.error(f"Failed to run bulk query: {e}")
        else:
            logging.error("Failed to run bulk query.")
        elapsed_time = time.time() - start_time
        logging.info(f"Time taken for bulk query: {elapsed_time} seconds")

    def bulk_update_for_large_csv(self, csv_filename, object_name, external_id):
        chunk_files = self.create_file_chunks(csv_filename)
        threads = []
        for chunk_file in chunk_files:
            t = threading.Thread(target=self.bulk_update, args=(object_name, external_id, chunk_file))
            t.start()
            threads.append(t)
        for t in threads:
            t.join()
        logging.info("All bulk updates completed.")

    def bulk_update(self, object_name, external_id, csv_filename):
        logging.info(f"Updating {object_name} with external ID {external_id} from {csv_filename}")
        start_time = time.time()
        command = f"sf data upsert bulk --sobject {object_name} --file {csv_filename} --external-id {external_id}"
        output = self.run_sfdx_command(command)
        if output:
            try:
                job_info = json.loads(output)
                self.is_bulk_upsert_done(job_info['result']['jobInfo']['id'])
            except Exception as e:
                logging.error(f"Failed to run bulk update: {e}")
        else:
            logging.error("Failed to run bulk upsert.")
        elapsed_time = time.time() - start_time
        logging.info(f"Time taken for bulk update: {elapsed_time} seconds")

    def bulk_delete_for_large_csv(self, object_name, csv_filename):
        chunk_files = self.create_file_chunks(csv_filename)
        threads = []
        for chunk_file in chunk_files:
            t = threading.Thread(target=self.bulk_delete, args=(object_name, chunk_file))
            t.start()
            threads.append(t)
        for t in threads:
            t.join()
        logging.info("All bulk deletes completed.")

    def create_file_chunks(self, csv_filename, output_dir=OUTPUT_DIR, max_size_mb=MAX_CHUNK_SIZE_MB):
        ensure_output_dir_is_empty(output_dir)
        self.split_csv_file(csv_filename, max_size_mb=max_size_mb)
        # List all chunk files
        chunk_files = [os.path.join(output_dir, f) for f in os.listdir(output_dir) if f.endswith('.csv')]
        # Ensure paths are handled correctly for shlex.split
        chunk_files = [f.replace('\\', '\\\\') for f in chunk_files]
        return chunk_files

    def bulk_delete(self, object_name, csv_filename):
        command = f"sf data delete bulk --sobject {object_name} --file {csv_filename}"
        output = self.run_sfdx_command(command)
        if output:
            try:
                job_info = json.loads(output)
                self.is_bulk_delete_done(job_info['result']['jobInfo']['id'])
            except Exception as e:
                logging.error(f"Failed to run bulk delete: {e}")
        else:
            logging.error("Failed to run bulk delete.")

    def get_result_from_bulk_query(self, job_id, file_path):
        base_url = f"{self.instance_url}/services/data/v{VERSION}/jobs/query/{job_id}/results"
        headers = {
            'Authorization': f"Bearer {self.access_token}",
            'Content-Type': 'text/csv; charset=UTF-8',
            'Accept-Encoding': 'gzip'
        }
        max_retries = 5
        retry_count = 0
        locator = None
        header_written = False

        while True:
            url = base_url
            if locator:
                url += f"?locator={locator}"

            response = requests.get(url, headers=headers)

            if response.status_code == 200:
                with io.StringIO(response.text) as csv_content:
                    csv_reader = csv.reader(csv_content)
                    with open(file_path, mode='a', newline='', encoding='utf-8') as csv_file:
                        csv_writer = csv.writer(csv_file)
                        for index, row in enumerate(csv_reader):
                            if index == 0:
                                if not header_written:
                                    csv_writer.writerow(row)  # Write header
                                    header_written = True
                            else:
                                csv_writer.writerow(row)
                logging.info(f"Data saved to {file_path}")
                locator = response.headers.get('Sforce-Locator')
                if not locator or locator == "null":
                    break
            elif response.status_code in [429, 500, 502, 503, 504]:
                time.sleep(5)
                retry_count += 1
                if retry_count >= max_retries:
                    logging.error("Exceeded maximum retries while fetching bulk query results.")
                    return False
            else:
                logging.error(f"Failed to retrieve job status: {response.text}")
                return False
        return True

    def is_bulk_query_done(self, job_id):
        url = f"{self.instance_url}/services/data/v{VERSION}/jobs/query/{job_id}"
        headers = {
            'Authorization': f"Bearer {self.access_token}",
            'Content-Type': 'application/json'
        }
        max_retries = 5
        retry_count = 0

        while True:
            response = requests.get(url, headers=headers)
            if response.status_code == 200:
                job_status = response.json()
                if job_status['state'] in ['JobComplete', 'Failed', 'Aborted']:
                    return True
                else:
                    time.sleep(SLEEP_TIME)
            elif response.status_code in [429, 500, 502, 503, 504]:
                time.sleep(5)
                retry_count += 1
                if retry_count >= max_retries:
                    logging.error("Exceeded maximum retries while checking bulk query status.")
                    return False
            else:
                logging.error(f"Failed to retrieve job status: {response.text}")
                return False

    def is_bulk_delete_done(self, job_id):
        return self.is_bulk_job_done(job_id)

    def is_bulk_upsert_done(self, job_id):
        return self.is_bulk_job_done(job_id)

    def is_bulk_job_done(self, job_id):
        url = f"{self.instance_url}/services/data/v{VERSION}/jobs/ingest/{job_id}"
        headers = {
            'Authorization': f"Bearer {self.access_token}",
            'Content-Type': 'application/json'
        }
        while True:
            response = requests.get(url, headers=headers)
            job_status = response.json()
            if job_status['state'] in ['JobComplete', 'Failed', 'Aborted']:
                return True
            else:
                time.sleep(SLEEP_TIME)

    @staticmethod
    def split_csv_file(input_file_path, output_dir=OUTPUT_DIR, max_size_mb=MAX_CHUNK_SIZE_MB):
        """Splits a large CSV file into smaller chunks based on the max_size_mb limit."""
        if not output_dir.exists():
            output_dir.mkdir(parents=True, exist_ok=True)

        current_chunk = 1
        current_size = 0

        with open(input_file_path, 'r', encoding='utf-8') as input_file:
            csv_reader = csv.reader(input_file)
            headers = next(csv_reader)

            output_file = open(output_dir / f'chunk_{current_chunk}.csv', 'w', newline='', encoding='utf-8')
            csv_writer = csv.writer(output_file)
            csv_writer.writerow(headers)

            for row in csv_reader:
                if current_size >= max_size_mb * 1024 * 1024:
                    output_file.close()
                    current_chunk += 1
                    current_size = 0
                    output_file = open(output_dir / f'chunk_{current_chunk}.csv', 'w', newline='', encoding='utf-8')
                    csv_writer = csv.writer(output_file)
                    csv_writer.writerow(headers)

                csv_writer.writerow(row)
                current_size += len(','.join(row).encode('utf-8'))

            output_file.close()

    def bulk_update_thread(self, object_name, external_id, csv_filename):
        """Wrapper for bulk_update to be used in a multithreaded environment."""
        try:
            self.bulk_update(object_name, external_id, csv_filename)
        except Exception as e:
            logging.error(f"Failed to update {object_name}: {e}")
