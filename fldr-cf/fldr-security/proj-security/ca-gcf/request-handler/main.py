import os
import json
# import logging
import requests
from datetime import datetime
import time
import jwt
import base64
import ipaddress
from collections import deque, defaultdict
import concurrent.futures
import pytz

from google.cloud import bigtable
from google.cloud import secretmanager
from google.auth import default  # compute_engine
from google.auth.transport.requests import Request
from google.oauth2.id_token import fetch_id_token
from google.cloud import compute_v1, cloudquotas_v1beta
from googleapiclient.discovery import build


credentials, _project_id = default()

# # Table IDs
# DATASET_ID: str = os.getenv("DATASET_ID_1")
# POLICY_RESOURCE_STATUS = os.getenv("POLICY_RESOURCE_STATUS")
# TABLE_ID: str = f'{_project_id}.{DATASET_ID}.{POLICY_RESOURCE_STATUS}'

# Initialize Secret Manager client (global to reuse across invocations)
secret_client = secretmanager.SecretManagerServiceClient()

ORG_ID: str = os.getenv("ORG_ID")
# Github App details - Moved to function scope for on-demand retrieval
APP_ID_SECRET_NAME: str = os.getenv("APP_ID_SECRET_NAME")  # Changed to str
PRIVATE_KEY_SECRET_NAME: str = os.getenv("PRIVATE_KEY_SECRET_NAME")
INSTALLATION_ID: str = os.getenv("INSTALLATION_ID") # Changed to str
# Supported Files
SUPPORTED_FILE_NAMES: list = os.getenv("SUPPORTED_FILE_NAMES").split("+")
# Allowed Rule File Names
ALLOWED_ACCESS_FILE_NAMES: list = os.getenv("ALLOWED_ACCESS_FILE_NAMES", []).split("+")
# Batch Size
BATCH_SIZE: int = int(os.getenv("BATCH_SIZE"))
print(f"[GENERIC] Batch size: {BATCH_SIZE}")

# GCP Function for Policy process
GCP_POLICY_URL: str = os.getenv("GCP_POLICY_URL")
GCP_POLICY_ENDPOINT: str = os.getenv("GCP_POLICY_ENDPOINT")
SEC_HEADER: str = os.getenv("SEC_HEADER")
# Quota details
DESIRED_CLOUD_ARMOR_TIER: str = os.getenv("DESIRED_CLOUD_ARMOR_TIER")
print(f"[GENERIC] Desired cloud armor tier: {DESIRED_CLOUD_ARMOR_TIER}")
SERVICE_NAME: str = os.getenv("SERVICE_NAME")
print(f"[GENERIC] Service name: {SERVICE_NAME}")
RULE_QUOTA_REQUEST_VALUE: int = int(os.getenv("RULE_QUOTA_REQUEST_VALUE"))
print(f"[GENERIC] Quota request value: {RULE_QUOTA_REQUEST_VALUE}")
QUOTA_ID: str = os.getenv("REGIONAL_QUOTA_ID")
print(f"[GENERIC] Quota id: {QUOTA_ID}")
GLOBAL_QUOTA_ID: str = os.getenv("GLOBAL_QUOTA_ID")
CONTACT_EMAIL: str = os.getenv("CONTACT_EMAIL")

PARENTS: list = os.getenv("PARENTS").split("_")

# New field: Indicate if the trigger was manual or automated
TRIGGER_TYPE_ENV_VAR = "TRIGGER_TYPE"
DEFAULT_TRIGGER_TYPE = "AUTOMATION"

#Bigtable Resource
PROJECT_ID = os.getenv("PROJECT_ID")
INSTANCE_ID = os.getenv("INSTANCE_ID")
T_ID = os.getenv("TABLE_ID")
COLUMN_FAMILY_ID = os.getenv("COLUMN_FAMILY_ID")


class BigtableUtility:
    def __init__(self, project_id, instance_id, table_id, column_family_id):
        self.project_id = project_id
        self.instance_id = instance_id
        self.table_id = table_id
        self.column_family_id = column_family_id
        self._connect()

    def _connect(self):
        client = bigtable.Client(project=self.project_id, admin=True)
        instance = client.instance(self.instance_id)
        self.table = instance.table(self.table_id)

    def upsert_rows_batch(self, data):
        """
        Upserts multiple rows into Bigtable with multiple columns.
        :param data: List of dicts with keys: 'row_key' (bytes), 'columns' (dict of {column_name: value})
        """
        rows = []
        for item in data:
            row = self.table.direct_row(item["row_key"])
            for col, val in item["columns"].items():
                row.set_cell(self.column_family_id, col, val)
            rows.append(row)
        self.table.mutate_rows(rows)

        return f"Batch of {len(rows)} rows inserted successfully."

    def upsert_single_row(self, row_key, column_data):
        """
        Upserts a single row with multiple columns.
        :param row_key: bytes
        :param column_data: dict of {column_name: value}
        """
        row = self.table.direct_row(row_key)
        for col, val in column_data.items():
            row.set_cell(self.column_family_id, col, val)
        row.commit()

        return f"Row with key {row_key} inserted successfully."

    def create_bigtable_batch_row_upsert_message(self, data: list[dict]):

        for item in data:
            item["row_key"] = item["row_key"].encode('utf-8')
            encoded_columns = {}
            for col, value in item.get("columns", {}).items():
                encoded_columns[col] = str(value).encode('utf-8')
            item["columns"] = encoded_columns

        self.upsert_rows_batch(data)

    def create_bigtable_single_row_upsert_message(self, project_id, region, columns: dict):
        """
        Encodes incoming parmas
        Upserts a single row with multiple columns.
        :param row_key: bytes
        :param column_data: dict of {column_name: value}
        """
        row_key = f"{project_id}#{region}".encode('utf-8')
        for col, value in columns.items():
            columns[col] = f'{value}'.encode('utf-8')

        self.upsert_single_row(row_key, columns)

    def scan_all_rows(self):
        """
        Scans and returns all rows from the Bigtable.
        """
        rows = []
        partial_rows = self.table.read_rows()
        partial_rows.consume_all()
        for row_key, row_data in partial_rows.rows.items():
            rows.append(row_data)
        return rows

bt = BigtableUtility(PROJECT_ID, INSTANCE_ID, T_ID, COLUMN_FAMILY_ID)


def get_current_timestamp():
    est_tz = pytz.timezone("US/Eastern")
    return datetime.now(est_tz).strftime("%Y-%m-%d %H:%M:%S.%f")


def total_run_time(func):
    """Decorator to log the total runtime of a function."""
    print(f"[DEBUG] Watching Total Run Time for: {func.__name__}")

    def wrapper(*args, **kwargs):
        start_time = datetime.now()
        result = func(*args, **kwargs)
        end_time = datetime.now()
        total_time = end_time - start_time
        print(f"[INFO] Execution completed for '{func.__name__}' - Total run time: {total_time}")
        return result

    return wrapper

def retry_on_exception(max_retries=3, delay_seconds=2, audit_message="Operation failed"):
    """
    Decorator for retrying a function call with exponential backoff and auditing failures.
    """
    def decorator(func):
        def wrapper(*args, **kwargs):
            for attempt in range(1, max_retries + 1):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    print(f"[WARNING] {audit_message} (Attempt {attempt}/{max_retries}): {e}")
                    if attempt < max_retries:
                        time.sleep(delay_seconds * (2 ** (attempt - 1)))  # Exponential backoff
                    else:
                        print(f"[CF1-ERROR] {audit_message} failed after {max_retries} attempts.")
                        raise
            return None
        return wrapper
    return decorator

@retry_on_exception(audit_message="Sending payload to target GCP function")
def send_payload_to_target_gcp_function(gcp_function_url, gcp_function_endpoint, request):
    """Send payload to target GCP function."""
    try:
        print(f"[INFO] Preparing to send payload to {gcp_function_url}")
        print(f"[DEBUG] Payload content: {request}")

        token = fetch_id_token(Request(), gcp_function_endpoint)
        print("[INFO] ID token generated successfully.")

        headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {token}",
            "Sec-Header": SEC_HEADER
        }

        print(f"[INFO] Sending request to {gcp_function_url}")
        requests.post(gcp_function_url, json=request, headers=headers, timeout=0.1)

        print(f"[SUCCESS] Request sent successfully to {gcp_function_url}")

    except requests.exceptions.Timeout:
        print(f"[INFO] Request fired and timeout expected. Successfully sent to {gcp_function_url}")
    except requests.exceptions.RequestException as e:
        print(f"[ERROR] HTTP request failed to {gcp_function_url}: {str(e)}")
        print(f"[CF1-ERROR] HTTP request failed to {gcp_function_url}: {str(e)}")
        raise
    except Exception as e:
        print(f"[ERROR] An unexpected error occurred while sending request to {gcp_function_url}: {str(e)}")
        print(f"[CF1-ERROR] Unexpected error occurred while sending request to {gcp_function_url}: {str(e)}")
        raise


def remove_and_validate_approved_regions(fetched_projects, filter_from_projects, validate=False):
    print(f"[INFO] Running remove_and_validate_approved_regions, validate={validate}")

    filter_dict = {item["project_id"]: set(item["regions"]) for item in filter_from_projects}
    filtered_projects = []

    for project in fetched_projects:
        project_id = project.get("project_id")
        approved_regions = filter_dict.get(project_id, set())

        if validate:
            remaining_regions = [region for region in project.get("regions", []) if region in approved_regions]
        else:
            remaining_regions = [region for region in project.get("regions", []) if region not in approved_regions]

        if remaining_regions:
            project_copy = project.copy()
            project_copy["regions"] = remaining_regions
            filtered_projects.append(project_copy)

    print(f"[INFO] Completed filtering. Projects remaining: {len(filtered_projects)}")
    return filtered_projects


def only_specific_projects(specific_projects, projects):

    filtered_specific_projects = []
    for project in projects:
        if project["project_id"] in specific_projects:
            filtered_specific_projects.append(project)

    return filtered_specific_projects


@retry_on_exception(audit_message="Retrieving project ID and region from Bigtable")
def get_projectid_region(approved_filter=False, is_active_filter=False):
    """
    Fetches project and region information from Bigtable based on filter conditions.
    Row key in Bigtable is expected to be 'project_id#region'.
    Columns like 'policy_id', 'is_active', 'license_status', 'quota_status', 'domain'
    are expected to be stored as UTF-8 strings.
    """

    print(f"[INFO][Bigtable] Retrieving records from table: {bt.table_id}. Approved: {approved_filter}, Active: {is_active_filter}")
    project_dict = {}
    
    cf_id_str = bt.column_family_id # This is a string

    try:
        # For Bigtable, complex server-side filtering like SQL is harder.
        # We'll scan and filter client-side. For very large tables, this could be slow/expensive.
        # Consider Bigtable filters for specific column value checks if performance becomes an issue.
        all_bt_rows = bt.scan_all_rows()
        
        for bt_row in all_bt_rows:
            row_key_str = bt_row.row_key.decode('utf-8', errors='ignore')
            parts = row_key_str.split('#', 1)
            if len(parts) != 2:
                print(f"[WARNING][Bigtable] Skipping malformed row key: {row_key_str}")
                continue
            project_id_from_key, region_from_key = parts

            # Helper to get cell value as string (assuming latest timestamp)
            def get_cell_val_str(column_name_str):
                column_name_bytes = column_name_str.encode('utf-8')
                cells = bt_row.cells.get(cf_id_str, {}).get(column_name_bytes, [])
                if cells:
                    return cells[0].value.decode('utf-8', errors='ignore')
                return None

            policy_id = get_cell_val_str("policy_id")
            is_active_str = get_cell_val_str("is_active") if get_cell_val_str("is_active") else "" # Expect "True" or "False" as strings
            license_status = get_cell_val_str("license_status")
            quota_status = get_cell_val_str("quota_status")
            domain = get_cell_val_str("domain")

            passes_filter = False
            if not approved_filter and not is_active_filter: # Default: policy_id IS NOT NULL
                if policy_id and policy_id != "NA": # Check for presence of meaningful policy_id
                    passes_filter = True
            
            elif is_active_filter: # is_active = TRUE
                if is_active_str == "True":
                    passes_filter = True
            
            elif approved_filter: # license_status = 'Approved' AND (quota_status = 'NA' OR quota_status = 'Approved')
                if license_status == 'Approved' and quota_status in ['NA', 'Approved']:
                    passes_filter = True
            
            if passes_filter:
                if project_id_from_key not in project_dict:
                    project_dict[project_id_from_key] = {
                        "project_id": project_id_from_key,
                        "regions": [],
                    }
                    # Add domain if this filter requires it and domain exists
                    if is_active_filter and domain:
                         project_dict[project_id_from_key]["domain"] = domain
                
                if region_from_key not in project_dict[project_id_from_key]["regions"]:
                    project_dict[project_id_from_key]["regions"].append(region_from_key)
                
                # Ensure domain is added if it's an active filter pass and domain exists for this project
                if is_active_filter and domain and "domain" not in project_dict[project_id_from_key]:
                     project_dict[project_id_from_key]["domain"] = domain


        print(f"[INFO][Bigtable] Fetched and filtered records. Projects count: {len(project_dict)}")
        return list(project_dict.values())

    except Exception as e:
        print(f"[CF1-ERROR] Failed fetching records from table: {str(e)}")
        raise


@retry_on_exception(audit_message="Fetching Cloud Armor service tier")
def get_license_request_info(project_id: str):
    try:
        print(f"[INFO] Fetching Cloud Armor service tier for project: {project_id}")

        client = compute_v1.ProjectsClient(credentials=credentials)
        request = compute_v1.GetProjectRequest(project=project_id)
        response = client.get(request=request)

        ca_tier = response.cloud_armor_tier
        print(f"[INFO] Current Cloud Armor Tier for {project_id}: {ca_tier}")
        return ca_tier

    except Exception as e:
        print(f"[CF1-ERROR][Project-id: {project_id}] Failed to fetch Cloud Armor tier info for {project_id}: {str(e)}")
        raise


@retry_on_exception(audit_message="Enrolling project to Cloud Armor")
def enroll_project_to_cloud_armor(project_id: str, tier: str):
    try:
        print(f"[INFO] Enrolling project '{project_id}' to Cloud Armor tier: {tier}")
        compute_client = compute_v1.ProjectsClient(credentials=credentials)

        tier_request_resource = compute_v1.ProjectsSetCloudArmorTierRequest(
            cloud_armor_tier=tier
        )

        tier_request = compute_v1.SetCloudArmorTierProjectRequest(
            project=project_id,
            projects_set_cloud_armor_tier_request_resource=tier_request_resource
        )

        response = compute_client.set_cloud_armor_tier(request=tier_request)

        if response.exception():
            print(f"[CF1-ERROR] Exception in response: {response.exception()}")
            raise Exception(f"Cloud Armor enrollment failed: {response.exception()}")

        if response.error_message:
            print(f"[CF1-ERROR] Error message in response: {response.error_message}")
            raise Exception(f"Cloud Armor error: {response.error_message}")

        print(f"[SUCCESS] Project '{project_id}' enrolled to tier: {tier}")
        return {"status": response.status.name, "end_time": response.end_time}

    except Exception as e:
        print(f"[CF1-ERROR][Project-id: {project_id}] Failed to enroll project '{project_id}' to tier '{tier}': {e}")
        raise


@retry_on_exception(audit_message="Fetching quota information")
def get_quota_info(project_id, service_name, quota_id, region):
    try:
        print(f"[INFO][Project-id: {project_id}, Region: {region}] Fetching quota info")

        client = cloudquotas_v1beta.CloudQuotasClient(credentials=credentials)

        regional = region

        if region.lower() == "global":
            quota_id = GLOBAL_QUOTA_ID

        # Step 2: Define the parent (project and region)
        path = f"projects/{project_id}/locations/global/services/{service_name}/quotaInfos/{quota_id}"

        # Step 3: List all quota information for the project and region
        request = cloudquotas_v1beta.GetQuotaInfoRequest(
            name=path
        )
        response = client.get_quota_info(request=request)

        # Step 4: Print the quota information
        temp = {"granted_value": 0}
        for quota_dimension in response.dimensions_infos:
            if regional == "global":
                if not quota_dimension.dimensions['region']:
                    temp = {
                        "granted_value": quota_dimension.details.value,
                    }
            else:
                if quota_dimension.dimensions['region'] == regional:
                    temp = {
                        "granted_value": quota_dimension.details.value,
                    }

        print(f"[SUCCESS][Project-id: {project_id}, Region: {region}] Quota info retrieved: {temp}")
        return temp

    except Exception as e:
        msg = {
            "latest_execution_status": "Failed",
            "failure_reason": f"Failed to fetch quota info: {str(e)}",
            "error_timestamp": get_current_timestamp()
        }
        print(f"[CF1-ERROR][Project-id: {project_id}, Region: {region}] Failed to fetch quota info for project {project_id}, region {region}: {str(e)}")
        bt.create_bigtable_single_row_upsert_message(project_id, region, msg)
        raise



@retry_on_exception(audit_message="Fetching quota preferences")
# Function to list quota preferences for a project in a specific region
def get_quota_preferences(project_id, region, quota_id):
    try:
        print(f"[INFO][Project-id: {project_id}, Region: {region}] Fetching quota preferences")

        client = cloudquotas_v1beta.CloudQuotasClient(credentials=credentials)
        parent = f"projects/{project_id}/locations/global"
        request = cloudquotas_v1beta.ListQuotaPreferencesRequest(parent=parent)
        response = client.list_quota_preferences(request=request)
        regional = region

        if region.lower() == "global":
            quota_id = GLOBAL_QUOTA_ID
            regional = {}

        # Step 4: Print the quota information
        temp = {}
        for quota_pref in response:
            if regional == "global":
                if not quota_pref.dimensions and quota_pref.quota_id == quota_id and quota_pref.quota_config.granted_value > 0:
                    temp = {
                        "name": quota_pref.name,
                        "preferred_value": quota_pref.quota_config.preferred_value,
                        "granted_value": quota_pref.quota_config.granted_value,
                        "region": "global",
                    }
            else:
                if quota_pref.quota_id == quota_id and regional == quota_pref.dimensions['region'] and quota_pref.quota_config.granted_value > 0:
                    temp = {
                        "name": quota_pref.name,
                        "preferred_value": quota_pref.quota_config.preferred_value,
                        "granted_value": quota_pref.quota_config.granted_value,
                        "region": quota_pref.dimensions.get('region', "global"),
                    }

        print(f"[INFO][Project-id: {project_id}, Region: {region}] Retrieved Quota Preference: {temp}")
        return temp

    except Exception as e:
        msg = {
            "latest_execution_status": "Failed",
            "failure_reason": f"Failed to fetch quota preferences: {str(e)}",
            "error_timestamp": get_current_timestamp()
        }
        print(f"[CF1-ERROR][Project-id: {project_id}, Region: {region}] Failed to fetch quota preferences for project {project_id}, region {region}: {str(e)}")
        bt.create_bigtable_single_row_upsert_message(project_id, region, msg)
        raise


@retry_on_exception(audit_message="Requesting quota increase")
def request_quota_increase_for_region(project_id, region, service_name, quota_id, requested_value, email, quota_pref):
    try:
        print(f"[INFO][Project-id: {project_id}, Region: {region}] Requesting quota increase")
        print(f"[DEBUG] Request Details - service: {service_name}, quota_id: {quota_id}, requested_value: {requested_value}")

        client = cloudquotas_v1beta.CloudQuotasClient(credentials=credentials)

        quota_preference = cloudquotas_v1beta.QuotaPreference()
        quota_preference.service = service_name
        quota_preference.quota_id = quota_id
        quota_preference.contact_email = email
        quota_preference.justification = "Need for increased capacity due to increased traffic"
        quota_preference.dimensions = {'region': region}
        quota_preference.name = f"projects/{project_id}/locations/global"

        if region.lower() == "global":
            quota_preference.quota_id = GLOBAL_QUOTA_ID
            del quota_preference.dimensions

        # Set the preferred quota configuration
        quota_preference.quota_config.preferred_value = requested_value  # Set the desired quota value

        # print(quota_preference)
        request = cloudquotas_v1beta.CreateQuotaPreferenceRequest(
            parent=f"projects/{project_id}/locations/global",
            quota_preference=quota_preference,
        )

        if quota_pref:
            print(f"[INFO][Project-id: {project_id}, Region: {region}] Updating existing quota preference")
            quota_preference.name = quota_pref["name"]
            request = cloudquotas_v1beta.UpdateQuotaPreferenceRequest(
                quota_preference=quota_preference,
            )
            response = client.update_quota_preference(request=request)
        else:
            # Step 3: Make the request to create quota preferences for the region
            response = client.create_quota_preference(request=request)

        print({
            "[GENERIC] message": f"Quota preference requested successfully for project: {project_id} in region: {region} with required value {requested_value}",
            "[INFO] response": {"created_time": response.create_time,
                         "name": response.name,
                         "preferred_value": response.quota_config.preferred_value,
                         "status": "Awaiting on approval"
                         }
        })

        return "Awaiting on approval"

    except Exception as e:
        msg = {
            "latest_execution_status": "Failed",
            "failure_reason": f"Failed quota increase request: {str(e)}",
            "error_timestamp": get_current_timestamp()
        }
        print(f"[CF1-ERROR][Project-id: {project_id}, Region: {region}] Failed quota increase request for project {project_id}, region {region}: {str(e)}")
        bt.create_bigtable_single_row_upsert_message(project_id, region, msg)
        raise Exception({
            "response": "error : " + str(e),
            "message": f"Failed to update quota in region: {region}"
        })


def request_quota_and_license_for_labeled_projects(projects, project_lic_quota_response, trigger_type = DEFAULT_TRIGGER_TYPE):
    """
    Processes quota increase and license tier changes for labeled projects.

    Args:
        projects (list): List of project dictionaries.
        trigger_type (str): Indicates the trigger type ("manual" or "automation").
    """
    try:
        print(f"[START] Processing quota and license updates for {len(projects)} projects concurrently.")

        def process_project(project):
            project_id = project.get("project_id")
            domain = project.get("domain")
            license_and_quota_responses = []

            print(f"[INFO][Project-id: {project_id}] Starting license processing.")
            l_status = process_license_for_project(project_id, trigger_type, domain)

            print(f"[INFO][Project-id: {project_id}] Starting quota processing.")
            quota_response = process_quota_for_project(project, trigger_type, l_status)

            if quota_response:
                print(f"[INFO][Project-id: {project_id}] Quota response collected.")
                license_and_quota_responses.extend(quota_response)
            else:
                print(f"[INFO][Project-id: {project_id}] No quota response received.")
            return license_and_quota_responses

        with concurrent.futures.ThreadPoolExecutor() as executor:
            futures = [executor.submit(process_project, project) for project in projects]
            for future in concurrent.futures.as_completed(futures):
                try:
                    result = future.result()
                    if result:
                        project_lic_quota_response["policy_rules"].append(result)
                except Exception as e:
                    print(f"[CF1-ERROR] Exception in concurrent processing: {str(e)}")

        print("[COMPLETE] License and quota requests submitted for all projects.")
        print("[INFO] Final payload:", json.dumps(project_lic_quota_response, indent=2))

        if project_lic_quota_response:
            print("[INFO] Sending payload to Bigtable update function.")

        return project_lic_quota_response

    except Exception as e:
        print(f"[FATAL][CF1-ERROR] Unexpected failure during quota/license processing: {str(e)}")
        return None


def process_license_for_project(project_id, trigger_type, domain):
    """
    Processes license tier changes for a specific project.

    Args:
        project_id (str): The ID of the project.
        trigger_type (str): The type of trigger.
    Returns:
        list: A list containing the license processing response.
    """
    responses = []
    try:
        print(f"[INFO][Project-id: {project_id}] Fetching current CA Tier.")
        ca_tier = get_license_request_info(project_id)
        print(f"[INFO][Project-id: {project_id}] Current CA Tier: {ca_tier}")

        if ca_tier != DESIRED_CLOUD_ARMOR_TIER:
            print(f"[ACTION][Project-id: {project_id}] Enrolling to CA Tier: {DESIRED_CLOUD_ARMOR_TIER}")
            response = enroll_project_to_cloud_armor(project_id, DESIRED_CLOUD_ARMOR_TIER)
        else:
            print(f"[SKIP][Project-id: {project_id}] Already at desired CA Tier.")
            response = {"status": "Approved"}

        ca_tier = get_license_request_info(project_id) # Get the tier again to ensure we have the latest value
        l_status = "Approved" if response['status'] == 'DONE' else response['status']

        print(f"[SUCCESS][Project-id: {project_id}] License processed successfully. Status: {l_status}")
    except Exception as e:
        responses.append({
            "project_id": project_id,
            "region": "NA",
            "action":"error",
            "comment":f"License processing error: {str(e)}",
            "trigger_type": trigger_type,
            "requested_time": f'{get_current_timestamp()}',
            "latest_execution_status": "Failed"
        })
        print(f"[CF1-ERROR][Project-id: {project_id}] License processing failed: {str(e)}")
        l_status = ("Failed",
                    f"License processing error: {str(e)}")

    return l_status


def process_quota_for_project(project, trigger_type, l_status):
    """
    Processes quota increase requests for a project.

    Args:
        project (dict):  project data.
        trigger_type (str): The type of trigger.

    Returns:
         list: A list containing the quota processing responses.
    """
    project_id = project.get("project_id")
    domain = project.get("domain")
    regions = project.get("regions", [])
    responses = []
    bigtable_rows_to_upsert = []

    for region in regions:
        row_key = f"{project_id}#{region}".encode('utf-8')
        column_data = {}
        try:
            print(f"[INFO][Project-id: {project_id}, Region: {region}] Checking current quota")
            quota_info = get_quota_info(project_id, SERVICE_NAME, QUOTA_ID, region)

            if quota_info['granted_value'] < RULE_QUOTA_REQUEST_VALUE:
                quota_preferences = get_quota_preferences(project_id, region, QUOTA_ID)
                print(f"[ACTION][Project-id: {project_id}, Region: {region}] Quota is insufficient. Requesting increase.")
                response = request_quota_increase_for_region(
                    project_id,
                    region,
                    SERVICE_NAME,
                    QUOTA_ID,
                    RULE_QUOTA_REQUEST_VALUE,
                    CONTACT_EMAIL,
                    quota_preferences
                )
            else:
                print(f"[SKIP][Project-id: {project_id}, Region: {region}] Quota is sufficient")
                response = "Approved"

            if not isinstance(l_status, str):

                column_data = {
                "project_id": project_id.encode('utf-8'),
                "region": region.encode('utf-8'),
                "license_status": l_status[0].encode('utf-8'),
                "creation_time": f'{get_current_timestamp()}'.encode('utf-8'),
                "disabled_time": "NA",
                "update_time": f'{get_current_timestamp()}'.encode('utf-8'),
                "trigger_type": trigger_type.encode('utf-8'),
                "latest_execution_status": "Failed".encode('utf-8'),
                "domain": domain.encode('utf-8'),
                "quota_status": str(response).encode('utf-8'),
                "failure_reason": l_status[1].encode('utf-8'),
                "error_timestamp": get_current_timestamp().encode('utf-8'),
                }
                bigtable_rows_to_upsert.append({"row_key": row_key, "columns": column_data})

            else:
                # Prepare data for Bigtable upsert
                column_data = {
                    "project_id": project_id.encode('utf-8'),
                    "region": region.encode('utf-8'),
                    "license_status": l_status.encode('utf-8'),
                    "creation_time": f'{get_current_timestamp()}'.encode('utf-8'),
                    "disabled_time": "NA",
                    "update_time": f'{get_current_timestamp()}'.encode('utf-8'),
                    # "request_response": str(response).encode('utf-8'),
                    # "comment": "Successfully processed quota increase".encode('utf-8'),
                    "trigger_type": trigger_type.encode('utf-8'),
                    "latest_execution_status": "Success".encode('utf-8'),
                    "domain": domain.encode('utf-8'),
                    "quota_status": str(response).encode('utf-8')
                }
                bigtable_rows_to_upsert.append({"row_key": row_key, "columns": column_data})

        except Exception as e:
            msg = {
                "creation_time": get_current_timestamp(),
                "latest_execution_status": "Failed",
                "failure_reason": f"Quota increase error: {str(e)}",
                "error_timestamp": get_current_timestamp(),
                "update_time": get_current_timestamp()
            }
            print(f"[CF1-ERROR][Project-id: {project_id}, Region: {region}] Quota processing failed: {str(e)}")
            bt.create_bigtable_single_row_upsert_message(project_id, region, msg)
            error_response = {
                "project_id": project_id,
                "region": region,
                "failure_reason": f"Quota increase error: {str(e)}",
                "trigger_type": trigger_type,
                "domain": domain,
                "latest_execution_status": "Failed"
            }
            responses.append(error_response)

            # For errors, upsert immediately as a single row to log the failure
            error_column_data = {
                "project_id": project_id.encode('utf-8'),
                "region": region.encode('utf-8'),
                "failure_reason": f"Quota increase error: {str(e)}".encode('utf-8'),
                "trigger_type": trigger_type.encode('utf-8'),
                "domain": domain.encode('utf-8'),
                "latest_execution_status": "Failed".encode('utf-8')
            }
            # bt.upsert_single_row(row_key, error_column_data)
            try:
                # Retry for Bigtable single upsert for error logging
                @retry_on_exception(max_retries=2, delay_seconds=1, audit_message=f"Bigtable single upsert for error for {project_id} {region}")
                def _upsert_error_to_bigtable():
                    bt.upsert_single_row(row_key, error_column_data)
                _upsert_error_to_bigtable()
                print(f"[INFO][Bigtable] Logged error for {project_id}, {region} to Bigtable.")
            except Exception as bt_e:
                print(f"[CF1-ERROR][Project-id: {project_id}, Region: {region}] Failed to upsert error data for {project_id}, {region} after retries: {str(bt_e)}")


    # Perform batch upsert if there are multiple successful regions, otherwise use single-row for individual successes
    if len(bigtable_rows_to_upsert) > 1:
        try:
            @retry_on_exception(max_retries=2, delay_seconds=1, audit_message=f"Bigtable batch upsert for project {project_id}")
            def _upsert_batch_to_bigtable():
                bt.upsert_rows_batch(bigtable_rows_to_upsert)
            _upsert_batch_to_bigtable()
            print(f"[INFO][Bigtable] Successfully batch upserted {len(bigtable_rows_to_upsert)} rows for project {project_id}.")
        except Exception as bt_e:
            msg = {
                "latest_execution_status": "Failed",
                "failure_reason": f"Failed to batch upsert: {str(bt_e)}",
                "error_timestamp": get_current_timestamp()
            }
            print(f"[CF1-ERROR][Bigtable][[Project-id: {project_id}, Region: {region}]] Failed to batch upsert for project {project_id} after retries: {str(bt_e)}")
            bt.create_bigtable_single_row_upsert_message(project_id, "unknown_region", msg)
            # Fallback to single row upserts if batch fails after retries
            for row_item in bigtable_rows_to_upsert:
                try:
                    @retry_on_exception(max_retries=5, delay_seconds=1, audit_message=f"Bigtable single upsert fallback for {row_item['row_key'].decode('utf-8')}")
                    def _upsert_single_fallback():
                        bt.upsert_single_row(row_item["row_key"], row_item["columns"])
                    _upsert_single_fallback()
                    print(f"[INFO][Bigtable] Successfully single upserted fallback for {row_item['row_key'].decode('utf-8')}.")
                except Exception as single_bt_e:
                    print(f"[CF1-ERROR][Bigtable] CRITICAL: Failed to single upsert fallback for {row_item['row_key'].decode('utf-8')} after retries: {str(single_bt_e)}")
    elif len(bigtable_rows_to_upsert) == 1:
        try:
            row_item = bigtable_rows_to_upsert[0]
            @retry_on_exception(max_retries=2, delay_seconds=1, audit_message=f"Bigtable single upsert for project {project_id}, region {row_item['row_key'].decode('utf-8').split('#')[1]}")
            def _upsert_single_to_bigtable():
                bt.upsert_single_row(row_item["row_key"], row_item["columns"])
            _upsert_single_to_bigtable()
            print(f"[INFO][Bigtable] Successfully single upserted for project {project_id}, region {row_item['row_key'].decode('utf-8').split('#')[1]}.")
        except Exception as bt_e:
            print(f"[CF1-ERROR][Project-id: {project_id}] Failed to single upsert for project {project_id} after retries: {str(bt_e)}")

    print(f"[INFO][Project-id: {project_id}] Completed quota processing for all regions {regions}.")
    return responses


# Main function: takes a single parent string and fetches labeled projects recursively
@retry_on_exception(audit_message="Fetching projects with labels from Cloud Asset API")
def fetch_projects_with_labels_nested_from_string(git=False):
    try:
        print("[START] Recursive fetch for labeled projects using Cloud Asset API")
        service = build('cloudasset', 'v1', credentials=credentials)

        all_projects = []
        page_token = None
        print(f"[GENERIC] scope:- {PARENTS}")
        projects = []

        count = 1
        for parent in PARENTS:
            print(f"[INFO] Starting search under parent: {parent}")
            while True:
                print(f"[INFO] Fetching page {count} for parent {parent}")
                request = service.v1().searchAllResources(
                    scope=parent,
                    assetTypes=['cloudresourcemanager.googleapis.com/Project'],
                    query='state:ACTIVE AND labels.ca_enterprise:*',
                    pageSize=100,
                    pageToken=page_token
                )
                response = request.execute()

                all_projects.extend(response.get('results', []))

                page_token = response.get('nextPageToken')
                if not page_token:
                    break
                count += 1

        seen_project_ids = set()  # Track added project IDs
        org_domain_cache = {}     # Cache for org_id → domain mapping
        org_service = build('cloudresourcemanager', 'v1', credentials=credentials)
        for project in all_projects:
            labels = project.get('labels')
            ca_scope_parts = []
            for x in range(1, 1000):
                val = labels.get(f"ca_scope_{x}")
                if val is None or val.lower() == "na":
                    break
                ca_scope_parts.append(val)
            ca_scope = "_".join(ca_scope_parts)
            ca_enterprise = labels.get('ca_enterprise', '').lower()
            priority = labels.get('priority', 'false').lower()
            organization_name = project.get('organization')
            org_id = organization_name.split('/')[-1]
            if org_id in org_domain_cache:
                domain = org_domain_cache[org_id]
                print(f"[INFO] Using cached domain for org_id: {org_id} - Domain: {domain}")
            else:
                try:
                    org_request = org_service.organizations().get(name=f'organizations/{org_id}')
                    org_response = org_request.execute()
                    domain = org_response.get('displayName', 'unknown').lower()
                    if domain == 'unknown':
                        print(f"[WARN] No display name found for org_id: {org_id}")
                except Exception as e:
                    print(f"[WARN] Failed to fetch organization name for org_id: {org_id}. Error: {e}")
                    domain = "unknown"
                org_domain_cache[org_id] = domain
            atr = project.get('additionalAttributes')
            project_id = atr.get("projectId")

            if ca_scope and ca_enterprise == 'true' and project_id not in seen_project_ids:
                seen_project_ids.add(project_id)
                print(f"[INFO][Project-id: {project_id}] Valid project found with CA scope and policy.")
                projects.append({
                    "project_number": project.get('project').split("/")[-1],
                    "organization": organization_name,
                    "domain": domain,
                    "project_id": project_id,
                    "regions": ca_scope.split('_'),
                    "priority": priority
                })

        print(f"[COMPLETE] Retrieved {len(projects)} valid labeled projects.")

        if git:
            projects.sort(key=lambda x: x['priority'] != 'true')

        return projects

    except Exception as e:
        print(f"[CF1-ERROR] Error occurred while fetching labeled projects: {str(e)}")
        raise


def update_is_active_status_for_projects(prj):
    """Mark all passed project-region combinations as active in Bigtable."""
    region_map = []
    for project in prj:
        project_id = project.get("project_id")
        regions= project.get("regions")
        domain= project.get("domain")
        for region in regions:
            is_active = True
            region_map.append({
                "row_key": f"{project_id}#{region}",
                "columns":{
                "project_id": project_id,
                "region": region,
                "is_active":is_active,
                "domain": domain
                }
            })
            print(f"[INFO][Project-id: {project_id}][Region: {region}] Marking as ACTIVE")
    print(f"[INFO] Project map for active regions: {region_map}")
    if region_map:
        print(f"[INFO] Sending active region payload to Bigtable for project {project_id}, column_data :{region_map}")
        bt.create_bigtable_batch_row_upsert_message(region_map)


def get_inactive_regions(fetched_projects, filter_from_projects):
    print("[INFO][GENERIC] Running get_inactive_regions")
    # Create a lookup: {project_id: set(regions)}
    # Normalize active dict
    active_dict = {
        item["project_id"]: set(r.strip().lower() for r in item["regions"])
        for item in filter_from_projects
    }

    inactive_projects = []
    for project in fetched_projects:
        project_id = project.get("project_id")
        all_regions = set(r.strip().lower() for r in project.get("regions", []))
        active_regions = active_dict.get(project_id, set())

        print(f"[DEBUG] Project: {project_id}, DB: {all_regions}, Active: {active_regions}")

        inactive_regions = list(all_regions - active_regions)
        if inactive_regions:
            print(f"[INFO][Project-id: {project_id}] Inactive regions found: {inactive_regions}")
            inactive_projects.append({
                "project_id": project_id,
                "regions": inactive_regions
            })

    print(f"[INFO] Completed. Projects with inactive regions: {len(inactive_projects)}")
    return inactive_projects

def mark_missing_regions_as_inactive(latest_active_projects):
    """
    Compares the latest active projects with the existing entries in Bigtable.
    Marks regions as inactive if they are no longer present in the active fetch.
    """
    project_is_active= get_projectid_region(is_active_filter=True)
    inactive_projects = get_inactive_regions(project_is_active, latest_active_projects)
    for project in inactive_projects:
        project_id = project.get("project_id")
        regions = project.get("regions")
        for region in regions:
            print(f"[INFO][Project-id: {project_id}][Region: {region}] Marking as INACTIVE")
            is_active = False
            msg = {
                "project_id": project_id,
                "region": region,
                "is_active": is_active,
                "disabled_time": get_current_timestamp()
            }
            bt.create_bigtable_single_row_upsert_message(project_id, region, msg)


@total_run_time
def auto_manage_cloud_armor_policies(request):
    try:
        request_json = parse_request_to_json(request)
    except ValueError as e:
        print(f"[CF1-ERROR] Failed to parse request: {e}")
        return "Failed to parse request"

    print("[INFO] Request received to auto-manage Cloud Armor policies.")

    # projects = fetch_projects_with_labels()
    # print(f"Fetched Projects are :- {projects}")
    git = True if request_json.get("sha") else False
    projects = fetch_projects_with_labels_nested_from_string(git)
    print(f"[GENERIC] Active Projects are :- {projects}")

    if projects:
        update_is_active_status_for_projects(projects)
        approved_projects = get_projectid_region( True)
        print(f"[GENERIC] Approved Projects are :- {approved_projects}")

        old_projects = get_projectid_region()
        print(f"[GENERIC] Old Projects are :- {old_projects}", )
        trigger_type = "AUTOMATION"
        if request_json.get("source"):
            handle_scheduler_trigger(request_json, projects, old_projects)
            trigger_type = "AUTOMATION"

        elif request_json.get("project_ids"):
            trigger_type = "Manual"
            print("[GENERIC] Trigger from curl")
            print(f"Project IDs are :- {request_json['project_ids']}")
            specific_projects_only = only_specific_projects(request_json["project_ids"], projects=projects)
            print("[GENERIC] Specific projects only", specific_projects_only)
            new_specific_projects = remove_and_validate_approved_regions(specific_projects_only, approved_projects)
            print("[GENERIC] New_specific_projects", new_specific_projects)
            project_lic_quota_response = {"policy_rules": []}
            if new_specific_projects:
                request_quota_and_license_for_labeled_projects(new_specific_projects, project_lic_quota_response, trigger_type="Manual")
                # pass
            # # Step 2 check current approved projects and identify if there are any new projects
            approved_projects = get_projectid_region(True)
            specific_projects = remove_and_validate_approved_regions(specific_projects_only, approved_projects, validate=True)
            print("[GENERIC] specific projects for further process", specific_projects)
            if specific_projects:
                # add logic to do a new project CA policy
                process_incoming_new_project_request(specific_projects, trigger_type="Manual")
            else:
                print("[GENERIC] No projects to incoming projects")

        elif request_json.get("sha"):
            print("[GENERIC] Trigger from git")
            print(f"[GENERIC] Github action metadata is :- {request_json}")
            if not request_json['files']:
                print(f"Invalid Git action provided please check the request and try again :- {request_json}")
            create_update_policies_payload_for_provision_function(projects, old_projects, "Github")
    else:
        print("[GENERIC] No active projects to automate cloud armor policy")

    # print("Successfully processed auto cloud armor policy management")
    try:
        mark_missing_regions_as_inactive(projects)
    except Exception as e:
        print(f"[CF1-ERROR[GENERIC] Failed to mark missing regions as inactive: {str(e)}")

    print("[COMPLETE][GENERIC] Successfully processed Cloud Armor policy automation.")
    return "Success"

def create_update_policies_payload_for_provision_function(projects, old_projects, trigger_type):
    print(f"[INFO][GENERIC] Current projects are :- {projects}")
    current_old_projects = remove_and_validate_approved_regions(projects, old_projects, validate=True)
    print(f"[INFO][GENERIC] Old projects to update policy rules are :- {current_old_projects}")
    if current_old_projects:
        process_incoming_new_project_request(current_old_projects, trigger_type=trigger_type)
    else:
        print("[INFO][GENERIC] No projects to update policy rules")


def create_json_for_apr_projects(status_response):

    project_region_map = defaultdict(set)

    for policy in status_response.get("policy_rules", []):
        for r in policy:
            if r.get("quota_status") in ["Approved", "NA"] and r.get("license_status") == "Approved":
                project_id = r.get("project_id")
                region = r.get("region")
                if project_id and region:
                    project_region_map[project_id].add(region)

    # Convert to desired format
    current_approved_projects = [
        {"project_id": pid, "regions": list(regions)}
        for pid, regions in project_region_map.items()
    ]

    return current_approved_projects


def parse_request_to_json(request):
    try:
        if not isinstance(request, dict):
            return request.get_json(force=True)
        return request
    except Exception:
        try:
            return json.loads(request)
        except Exception as e:
            print(f"[CF1-ERROR] Invalid incoming request, should be JSON: {request}")
            raise ValueError("Incorrect request format", str(e))


def handle_scheduler_trigger(request_json, projects, old_projects):
    print(f"[INFO][GENERIC] Trigger from scheduler: {request_json.get('source')}")

    if request_json.get("source", "").lower() != 'scheduler':
        print(f"[WARNING][GENERIC] Invalid scheduler source trigger: {request_json}")
        return

    approved_projects = get_projectid_region(True)
    print(f"[INFO][GENERIC] Approved projects count: {len(approved_projects)}")

    filtered_projects = remove_and_validate_approved_regions(projects, approved_projects)
    print(f"[INFO][GENERIC] Projects needing license/quota: {filtered_projects}")

    project_lic_quota_response = {"policy_rules": []}

    if filtered_projects:
        request_quota_and_license_for_labeled_projects(filtered_projects, project_lic_quota_response)

    approved_projects = get_projectid_region(True)

    new_projects = remove_and_validate_approved_regions(approved_projects, old_projects)
    current_new_projects = remove_and_validate_approved_regions(projects, new_projects, validate=True)

    if current_new_projects:
        print(f"[INFO][GENERIC] New projects to apply policy: {current_new_projects}")
        process_incoming_new_project_request(current_new_projects, "Scheduler")

    else:
        print("[INFO][GENERIC] No new projects to process")

    create_update_policies_payload_for_provision_function(projects, old_projects, "Scheduler")


# auto_manage_cloud_armor_policies({"source": "scheduler"})
def access_secret(secret_name):
     response = secret_client.access_secret_version(name=secret_name)
     return response.payload.data.decode('UTF-8')


def get_github_app_token(app_id, private_key, installtion_id):
     """
     Retrieve the GitHub App installtion token.
     """
     try:
         now = int(time.time())
         payload = {"iat": now, "exp": now + (10 * 60), "iss": app_id}
         print("[GENERIC] Get github token")
         jwt_token = jwt.encode(payload, private_key, algorithm="RS256")

         headers = {
             "Authorization": f"Bearer {jwt_token}",
             "Accept": "application/vnd.github+json"
         }
         url = f"https://api.github.com/app/installations/{installtion_id}/access_tokens"

         response = requests.post(url, headers=headers)
         response.raise_for_status()

         return response.json()["token"]

     except requests.exceptions.RequestException as e:
         print(f"[CF1-ERROR] GitHub API request failed in get_github_app_token: {str(e)}")
         raise # Re-raise for retry decorator
     except jwt.PyJWTError as e:
         print(f"[CF1-ERROR] JWT generation failed in get_github_app_token: {str(e)}")
         raise # Re-raise for retry decorator


@retry_on_exception(audit_message="Pulling file content from GitHub")
def pull_file_from_github(file_path, token):
     """
     Pull File content from the GitHub repository.
     """
     try:
         headers = {"Authorization": f"Bearer {token}", "Accept": "application/vnd.github+json"}
         print(f"[GENERIC] Pulling File content from GitHub: {file_path}")
         response = requests.get(file_path, headers=headers)
         response.raise_for_status()

         file_content = response.json().get("content")
         if file_content:
             str_ip_add = base64.b64decode(file_content).decode("utf-8").strip()
             ip_addresses = []
             for ip_add in str_ip_add.split("\n"):
                 ip_addresses.append(ip_add.strip())
             return ip_addresses

         return ""

     except requests.exceptions.RequestException as e:
         print(f"[CF1-ERROR] GitHub API request failed in pull_file_from_github: {str(e)}")
         raise # Re-raise for retry decorator


def get_files_from_github():
    try:
        private_key = access_secret(PRIVATE_KEY_SECRET_NAME)
        token = get_github_app_token(APP_ID_SECRET_NAME, private_key, INSTALLATION_ID)

        ip_address_groups = []
        for file in SUPPORTED_FILE_NAMES:  # Using .get() for safety
            file_path = f"https://api.github.com/repos/vonage-atmos/atmos-torg-geoip-propagation/contents/{file}.txt"

            ip_addresses = pull_file_from_github(file_path, token)

            if ip_addresses:
                ip_address_groups.append({
                    "rule": "Deny",
                    "group_name": file,
                    "ip_addresses": ip_addresses
                })

        policy_json = {"address_groups": ip_address_groups}

        print(f"[GENERIC] Fetched files from github repo: {policy_json}")
        return policy_json
    except Exception as e:
        print(f"[CF1-ERROR] Failed to get files from GitHub: {str(e)}")
        # Depending on criticality, you might re-raise or return an empty/default structure
        return {"address_groups": []}


def is_valid_ip(ip):
    """
    Validate whether the provided IP address is valid using the ipaddress module.
    """
    try:
        # Try to create an ip_address object from the string, which will raise an exception if invalid
        net = ipaddress.ip_network(ip, strict=False)
        return isinstance(net, ipaddress.IPv4Network)
    except ValueError:
        return False


def process_ip_addresses_into_valid_and_invalid(ip_group):
    """
    Processes the payload by cleaning data and separating valid IP addresses from invalid ones.
    """

    # Extract the payload from the request and validate payload data
    invalid_data = []  # To track invalid IPs
    valid_ips = []  # To hold valid IPs

    try:

        # Validate IP addresses and separate invalid ones
        valid_ips = [ip for ip in ip_group if is_valid_ip(ip)]

        # Track invalid IPs with the line number
        invalid_data = [{str(i+1): ip} for i, ip in enumerate(ip_group) if not is_valid_ip(ip)]

        return valid_ips, invalid_data

    except Exception as e:
        print("[CF1-ERROR] Error occurred while validation", str(e))
        return [], [] # return empty list in case of error


def process_incoming_new_project_request(projects, trigger_type= DEFAULT_TRIGGER_TYPE):
    """
    Processes a request to create Cloud Armor policies for new projects.

    Args:
        projects (list): A list of project dictionaries.
    """
    try:
        token = get_github_app_token(APP_ID_SECRET_NAME, access_secret(PRIVATE_KEY_SECRET_NAME), INSTALLATION_ID)

        ip_address_groups = []
        for file in SUPPORTED_FILE_NAMES:  # Using .get() for safety
            file_path = f"https://api.github.com/repos/vonage-atmos/atmos-torg-geoip-propagation/contents/{file}.txt"

            ip_addresses = pull_file_from_github(file_path, token)
            file_rule = "allow" if file in ALLOWED_ACCESS_FILE_NAMES else "deny"

            ip_addresses, invalid_data = process_ip_addresses_into_valid_and_invalid(ip_addresses)

            file = file.lower()

            if invalid_data:
                ip_address_groups.append({
                    "rule": file_rule,
                    "group_name": file,
                    "ip_addresses": ip_addresses,
                    "invalid_ips": invalid_data
                })
            else:
                ip_address_groups.append({
                    "rule": file_rule,
                    "group_name": file,
                    "ip_addresses": ip_addresses
                })

        print(f"[GENERIC] Policy will be processed on projects :- {projects}")

        batch_run_policy_process(projects, ip_address_groups, trigger_type=trigger_type)

    except Exception as e:
        print(f"[CF1-ERROR] Failed to process incoming new project request: {str(e)}")
        # This failure should ideally be audited, e.g., to Bigtable.


def batch_run_policy_process(projects, ip_address_groups, trigger_type=DEFAULT_TRIGGER_TYPE):
    try:
        batch_size = BATCH_SIZE
        # Step 1: Flatten all (project_id, region) pairs
        pairs = []
        for entry in projects:
            project_id = entry['project_id']
            for region in entry['regions']:
                pairs.append((project_id, region))

        # Step 2: Deduplicate and load into queue
        seen = set()
        queue = deque()
        for pid, region in pairs:
            if (pid, region) not in seen:
                seen.add((pid, region))
                queue.append((pid, region))

        # Step 3: Build batches with the batching logic
        batches = []

        while queue:
            current_batch = []
            batch_project_map = defaultdict(list)

            # Step 3a: Take first item as batch owner
            pid0, region0 = queue.popleft()
            batch_project_map[pid0].append(region0)
            current_batch.append((pid0, region0))

            # Step 3b: Fill batch up to 5 pairs
            while queue and len(current_batch) < batch_size:
                pid, region = queue[0]
                if pid == pid0 or len(batch_project_map[pid0]) < batch_size:
                    pid, region = queue.popleft()
                    batch_project_map[pid].append(region)
                    current_batch.append((pid, region))
                else:
                    break  # Don't include other project_id if batch_project_id has 5 already

            # Step 3c: Format result
            formatted_batch = []
            for pid, regions in batch_project_map.items():
                formatted_batch.append({'project_id': pid, 'regions': sorted(set(regions))})

            batches.append(formatted_batch)

        # Step 4: Output
        for i, batch in enumerate(batches, 1):
            print("[GENERIC] Sending batch wise request to CF2")
            print(f"[GENERIC] Batch {i}: {batch}")
            project_pre_run_info(batch, trigger_type)
            send_payload_to_target_gcp_function(GCP_POLICY_URL, GCP_POLICY_ENDPOINT, {"projects": batch, "address_groups": ip_address_groups})
        print("[GENERIC] All Batch request are sent successfully")
    except Exception as e:
        print(f"[CF1-ERROR] Failed during batch run policy process: {str(e)}")
        # This failure should also be audited.


def project_pre_run_info(projects, trigger_type):
    region_map = []
    for project in projects:
        project_id = project.get("project_id")
        regions = project.get("regions")
        for region in regions:
            latest_execution_status = "In_progress"
            is_active = True
            region_map.append(
                {
                "row_key": f"{project_id}#{region}",
                "columns":
                {
                "latest_execution_status": latest_execution_status,
                "is_active": is_active,
                "trigger_type": trigger_type
                }
            })
        print(f"[GENERIC] Project_mapping: {region_map}")

    bt.create_bigtable_batch_row_upsert_message(region_map)
    return region_map
