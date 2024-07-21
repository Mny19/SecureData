# SecureData
The SecureData project is designed to enhance the efficiency, scalability, and security of data storage, querying, and analysis within BigQuery environments, specifically for cybersecurity data management.

## Prerequisites

1. Google Cloud Project with BigQuery and KMS APIs enabled.
2. Google Cloud SDK installed.
3. Authentication setup with appropriate permissions.
4. Python 3.x installed.
5. Required Python packages:
   - google-cloud-bigquery
   - google-cloud-kms


## Setup Instructions

1. **Clone the repository:**
   ```sh
   git clone https://github.com/your-username/your-repo.git
   cd your-repo
   ```

2. **Install required packages:**
   ```sh
   pip install google-cloud-bigquery google-cloud-kms
   ```

3. **Set environment variables:**
   ```sh
   export PROJECT_ID="your-project-id"
   export KEY_RING_ID="your-key-ring-id"
   export CRYPTO_KEY_ID="your-crypto-key-id"
   ```


## Usage

### securedata.py

This script configures BigQuery, creates tables, loads data, performs encryption, and runs performance tests.

1. **Run the script:**
   ```sh
   python securedata.py
   ```

### dataset.py

This script lists tables within a specified dataset.

1. **Run the script:**
   ```sh
   python dataset.py
   ```

Ensure that the `PROJECT_ID`, `KEY_RING_ID`, and `CRYPTO_KEY_ID` environment variables are set correctly before running the scripts.
