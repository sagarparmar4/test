# S3 Copy Script

This Python script copies objects from one Amazon S3 bucket to another using asynchronous operations with `aioboto3`. The script uses AWS STS to assume roles for source and destination accounts and processes the objects in batches for efficiency.

## Features
- Asynchronous object copying using `aioboto3`.
- Batching for performance.
- Logs to console and file for tracking progress and errors.
- Error handling for network issues.

## Prerequisites
- Python 3.7 or later
- AWS account with appropriate IAM roles and S3 buckets

## Setup

1. Clone this repository:
   ```bash
   git clone <repository-url>
   cd <repository-directory>
   ```

2. Install the required packages:
   ```bash
   pip install -r requirements.txt
   ```

3. Update the configuration in `s3_copy_script.py`:
   - Replace `SOURCE_ACCOUNT_ID`, `DESTINATION_ACCOUNT_ID`, and bucket names with your actual values.

## Running the Script
To execute the script, run:
```bash
python s3_copy_script.py
```

## Running Tests
To run the tests, execute:
```bash
python -m unittest test_s3_copy_script.py
```

## Logging
Logs will be generated in the `s3_copy.log` file and also displayed in the console.

## License
This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.
