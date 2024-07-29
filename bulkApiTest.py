import csv
import os
import threading

from services.sfUtility import SFUtility

RECORDS_CSV_FROM_SF = 'LargeDataRecordsFromSF.csv'
RECORDS_CSV = 'LargeDataRecords.csv'

sf_utility = SFUtility('datacloud')
sf_utility.bulk_query('Select Id, ExternalId__c, Test123__c From LargeDataRecord__c', RECORDS_CSV_FROM_SF)
# sf_utility.bulk_delete_for_large_csv('LargeDataRecord__c', RECORDS_CSV)

# sf_utility.bulk_update('LargeDataRecord__c', 'external_id', 'LargeDataRecords.csv')
# sf_utility.bulk_update_for_large_csv(RECORDS_CSV, 'LargeDataRecord__c', 'ExternalId__c')

