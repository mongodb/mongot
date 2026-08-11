import argparse
import glob
import json

from pymongo import MongoClient


def parse_arguments():
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(
        description="Upload structured MongoDB logs to a MongoDB collection."
    )
    parser.add_argument("--log_file", type=str, required=True, help="Path to the log file.")
    parser.add_argument(
        "--connection_string",
        type=str,
        required=True,
        help="MongoDB connection string (i.e., mongodb://localhost:37017/).",
    )
    parser.add_argument(
        "--db_name",
        type=str,
        default="mongot_logs",
        help="Name of the MongoDB database to upload logs to. Defaults to 'mongot_logs'.",
    )
    parser.add_argument(
        "--collection_name",
        type=str,
        default="logs",
        help="Name of the MongoDB collection to upload logs to. Defaults to 'logs'.",
    )
    return parser.parse_args()


def upload_logs(log_file_path, connection_string, db_name, collection_name):
    """Upload logs from the specified file to the MongoDB collection."""
    # Connect to MongoDB
    client = MongoClient(connection_string)
    db = client[db_name]
    collection = db[collection_name]

    # Read and upload logs
    log_files = glob.glob(log_file_path)
    for log_file in log_files:
        with open(log_file, "r") as file:
            for line in file:
                try:
                    log_entry = json.loads(line)
                    collection.insert_one(log_entry)
                except json.JSONDecodeError as e:
                    print(f"Failed to decode JSON from line: {line.strip()}. Error: {e}")


def main():
    args = parse_arguments()
    upload_logs(args.log_file, args.connection_string, args.db_name, args.collection_name)


if __name__ == "__main__":
    main()
