# sf-python-ldv

Usage
The SFUtility class provides a set of tools to efficiently manage large-scale data operations in Salesforce. It supports bulk querying, updating, and deleting records, overcoming the limitations of Salesforce's Data Loader.

Prerequisites
- Python 3.x
- sf CLI installed and configured

Python code to run (bulkAPITest.py)

```Copy code
import csv
import os
import threading
from services.sfUtility import SFUtility

# Define file paths
RECORDS_CSV_FROM_SF = 'LargeDataRecordsFromSF.csv'
RECORDS_CSV = 'LargeDataRecords.csv'

# Initialize the SFUtility class with your Salesforce alias
sf_utility = SFUtility('datacloud')

# Perform a bulk query to retrieve data
sf_utility.bulk_query('SELECT Id, ExternalId__c, Test123__c FROM LargeDataRecord__c', RECORDS_CSV_FROM_SF)

# Bulk delete records from Salesforce
sf_utility.bulk_delete_for_large_csv('LargeDataRecord__c', RECORDS_CSV)

# Bulk update records in Salesforce
sf_utility.bulk_update_for_large_csv(RECORDS_CSV, 'LargeDataRecord__c', 'ExternalId__c')
```

# Configuration

- Chunk Size: The maximum size for each chunk of data can be configured in the script. The default is set to 100 MB.
- Logging: Logs are saved to sfUtility.log. You can adjust the logging level in the script.

# Contributing
Contributions are welcome! Please fork the repository and submit a pull request for any enhancements or bug fixes.

License
This project is licensed under the MIT License - see the LICENSE file for details.

