# -------------------------------------------------------------------------
#
# THIS CODE AND INFORMATION ARE PROVIDED "AS IS" WITHOUT WARRANTY OF ANY KIND,
# EITHER EXPRESSED OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE IMPLIED WARRANTIES
# OF MERCHANTABILITY AND/OR FITNESS FOR A PARTICULAR PURPOSE.
# ----------------------------------------------------------------------------------
# The example companies, organizations, products, domain names,
# e-mail addresses, logos, people, places, and events depicted
# herein are fictitious. No association with any real company,
# organization, product, domain name, email address, logo, person,
# places, or events is intended or should be inferred.
# --------------------------------------------------------------------------

# Global constant variables (Azure Storage account/Batch details)

# import "config.py" in "spc-hpc-client.py "
# Please note that storing the batch and storage account keys in Azure Key Vault
# is a better practice for Production usage.

"""
Configure Batch and Storage Account credentials
"""

BATCH_ACCOUNT_NAME = 'spcbatch'  # Your batch account name
BATCH_ACCOUNT_KEY = '3dZPGc9xW7dmmZgzDeWdTaE2BVQlsWEi6mbJy5p0BuoO4ZSFRg5qNZd5rQk5w0SEIPCE2hX/MiC3+ABaCcKdzg=='  # Your batch account key
BATCH_ACCOUNT_URL = 'https://spcbatch.uksouth.batch.azure.com'  # Your batch account URL
STORAGE_ACCOUNT_NAME = 'scpoutputs'
STORAGE_ACCOUNT_KEY = '+Qh7gN8zyl8iBPfKAgaUp0FtQlDu9vPsNSCTnSzJY3Nc/ZNJXYCCzKqsN0YSLDSBQpQiCYmebAco+AStg6MU9w=='
STORAGE_ACCOUNT_DOMAIN = 'blob.core.windows.net' # Your storage account blob service domain

POOL_ID = 'englandtest'  # Your Pool ID
POOL_NODE_COUNT = 4  # Pool node count
POOL_VM_SIZE = 'STANDARD_A1_V2'  # VM Type/Size
JOB_ID = 'englandtest'  # Job ID
STANDARD_OUT_FILE_NAME = 'stdout.txt'  # Standard Output file