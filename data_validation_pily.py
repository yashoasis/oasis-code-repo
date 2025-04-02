import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import pandas as pd
from io import StringIO
import boto3
import json

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Predefined column headers from provided documents
doc1 = """FAC_CD,DTA_TYP_CD,ACCTG_YR,ACCTG_PRD_NO,JE_NO,IP_ENT_NO,JE_GRP_NO,SEQ_NO,PCTR_FAC_CD,PCTR_LWR_TIER_1,PCTR_LWR_TIER_2,FNCL_ORG_CD,FNCL_ORG_TYP,NEW_FNCL_ACCT_NO,BDGT_SRC_CD,PRD_VAR_CD,CCY_CD,SRC_CCY_CD,ACCTG_ORD_NO,LBR_OR_MATL_IND,CTL_ACCT_NO,JE_DR_AMT,RCVBL_INVC_CTL_NO,JE_NM,NON_US_LOCL_CD,TX_CD,CNVRT_RATE,CCY_CNVRT_DT,TRNSLT_CCY_CD,TRNSLT_AMT,VAT_AMT,PRCH_DT,CCY_RATE_TYP_CD,SRC_AMT,FOGAP_SRC_IND,DEPT_NO,CCY_TRNSLT_DT,TRNSLT_RATE,BOOK_DT,DUE_DT,INVC_TYP,INVC_DT,INVC_LN_NO,TXT,SUPP_INVC_NO,ISU_FAC_CD,CAT_ID_NO,CAT_ID_CLS_CD,PROD_SER_NO,RE_PGM_MISC_CD,RE_PGM_FNCT_TYP,RE_PGM_WRK_PHS,RE_PGM_RSRC_TYP,PO_NO,ITM_QTY,UM_CD,SLS_TX_CD,TX_PCT,SHP_DT,SUPP_CD,SUPP_NM,OWN_FNCL_ORG_CD,OWNER_CLS_CD,LAST_UPDT_ACTV_ID,UPDT_USER_LOGON_ID,LAST_UPDT_TS,CTRCT_FILE_NO,SF_CDC_TS"""  # Account_payable

doc2 = """FAC_CD,DTA_TYP_CD,ACCTG_YR,ACCTG_PRD_NO,JE_NO,IP_ENT_NO,JE_GRP_NO,SEQ_NO,PCTR_FAC_CD,PCTR_LWR_TIER_1,PCTR_LWR_TIER_2,FNCL_ORG_CD,FNCL_ORG_TYP,NEW_FNCL_ACCT_NO,BDGT_SRC_CD,PRD_VAR_CD,CCY_CD,SRC_CCY_CD,ACCTG_ORD_NO,LBR_OR_MATL_IND,CTL_ACCT_NO,JE_DR_AMT,RCVBL_INVC_CTL_NO,JE_NM,NON_US_LOCL_CD,TX_CD,CNVRT_RATE,CCY_CNVRT_DT,TRNSLT_CCY_CD,TRNSLT_AMT,VAT_AMT,PRCH_DT,CCY_RATE_TYP_CD,SRC_AMT,FOGAP_SRC_IND,DEPT_NO,CCY_TRNSLT_DT,TRNSLT_RATE,DOC_DT,EMP_ID_NO,EMP_LAST_NM,EMP_FIRST_NM,EMP_MID_INIT,WRK_PH_NO,EXP_BK_CFIRM_NO,SPL_HDL_CD,ACTV_PRD_STRT_DT,CASH_IND,EXP_ACCT_TYP,EXP_BK_TYP,EXP_RPT_FLD_ID,LAST_UPDT_ACTV_ID,UPDT_USER_LOGON_ID,LAST_UPDT_TS,SF_CDC_TS"""  # Expense Book

doc3 = """FAC_CD,DTA_TYP_CD,ACCTG_YR,ACCTG_PRD_NO,JE_NO,IP_ENT_NO,JE_GRP_NO,SEQ_NO,PCTR_FAC_CD,PCTR_LWR_TIER_1,PCTR_LWR_TIER_2,FNCL_ORG_CD,FNCL_ORG_TYP,NEW_FNCL_ACCT_NO,BDGT_SRC_CD,PRD_VAR_CD,CCY_CD,SRC_CCY_CD,ACCTG_ORD_NO,LBR_OR_MATL_IND,CTL_ACCT_NO,JE_DR_AMT,RCVBL_INVC_CTL_NO,JE_NM,NON_US_LOCL_CD,TX_CD,CNVRT_RATE,CCY_CNVRT_DT,TRNSLT_CCY_CD,TRNSLT_AMT,VAT_AMT,PRCH_DT,CCY_RATE_TYP_CD,SRC_AMT,FOGAP_SRC_IND,DEPT_NO,CCY_TRNSLT_DT,TRNSLT_RATE,PROD_SER_NO,SLS_MDL_NO,CAT_ID_NO,CUST_CD,MFG_FAC_CD,BUY_FAC_CD,JE_HR,PART_PROD_CD,JE_MISC_QTY,SUPP_ID_CD,LAST_UPDT_ACTV_ID,UPDT_USER_LOGON_ID,LAST_UPDT_TS,OWN_FNCL_ORG_ORG,OWNER_CLS_CD,SF_CDC_TS"""  # Miscellaneous

# Initialize S3 client
s3_client = boto3.client('s3')

# S3 bucket and specific file paths
bucket_name = "pily-raw-data"
s3_files = [
    "ja/pily_raw_ap_det/AP_DET.csv",      # For doc1
    "ja/pily_raw_exp_det/EXP_DET.csv",    # For doc2
    "ja/pily_raw_fogap_det/FOGAP_DET.csv"  # For doc3
]

# Document names and corresponding expected headers
doc_names = ["Account_payable", "Expense Book", "Miscellaneous"]
documents = [doc1, doc2, doc3]

# Function to get the header from a specific S3 file
def get_header_from_s3(bucket, file_key):
    try:
        obj = s3_client.get_object(Bucket=bucket, Key=file_key)
        header = obj['Body']._raw_stream.readline().decode('utf-8').strip()
        return header
    except Exception as e:
        print(f"Error fetching header from {file_key}: {e}")
        return None

# Validation logic
print("Validating column headers against S3 files...\n")
errors = 0

for expected_doc, s3_file, name in zip(documents, s3_files, doc_names):
    # Parse expected columns from predefined header
    expected_df = pd.read_csv(StringIO(expected_doc))
    expected_cols = list(expected_df.columns)
    expected_count = len(expected_cols)
    
    # Fetch S3 header
    s3_header = get_header_from_s3(bucket_name, s3_file)
    if not s3_header:
        print(f"Skipping validation for {name} due to S3 fetch failure.")
        errors += 1
        print()
        continue
    
    # Parse S3 columns
    s3_df = pd.read_csv(StringIO(s3_header))
    s3_cols = list(s3_df.columns)
    s3_count = len(s3_cols)
    
    # Display and compare
    print(f"Comparing {name} with {s3_file}:")
    print(f"Expected columns from {name} ({expected_count}): {', '.join(expected_cols)}")
    print(f"S3 columns from {s3_file} ({s3_count}): {', '.join(s3_cols)}")
    
    if s3_count != expected_count:
        print(f" - Error: Column count mismatch ({s3_count} vs {expected_count})")
        errors += 1
    
    if set(s3_cols) != set(expected_cols):
        missing_cols = set(expected_cols) - set(s3_cols)
        extra_cols = set(s3_cols) - set(expected_cols)
        if missing_cols:
            print(f" - Error: Missing columns: {', '.join(missing_cols)}")
            errors += 1
        if extra_cols:
            print(f" - Error: Extra columns: {', '.join(extra_cols)}")
            errors += 1
    
    if s3_count == expected_count and set(s3_cols) == set(expected_cols):
        print(" - No issues found.")
    print()

# Summary
print(f"Total errors found: {errors}")
if errors == 0:
    print(json.dumps({"validation_status": "SUCCESS"}))  # Output SUCCESS status as JSON
    print("All document headers match their respective S3 files!")
else:
    print(json.dumps({"validation_status": "FAILED", "error_count": errors}))  # Output FAILURE status and error count
    print("Please review the errors above.")
	
job.commit()