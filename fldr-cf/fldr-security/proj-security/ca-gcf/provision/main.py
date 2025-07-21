import asyncio
import os
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime

import pytz
from google.cloud import bigtable, compute_v1

# OWASP RULE PRIORITY
WAF_RULE_PRIORITY: int = int(os.getenv("INITIAL_PRIORITY_FOR_OWASP_RULE"))
waf_rule_priority = WAF_RULE_PRIORITY

# OWASP RULES TO APPLY FOR EACH POLICY
OWASP_RULE: str = os.getenv("OWASP_RULE")

IP_CHUNK_SIZE: int = int(os.getenv("IP_CHUNK_SIZE"))

SUPPORTED_FILE_NAMES: list = [
    x.lower() for x in os.getenv("SUPPORTED_FILE_NAMES").split("+")
]
print(f"[GENERIC] Supproted file names / group name are :- {SUPPORTED_FILE_NAMES}")

REGIONAL_RULE_INITIAL_PRIORITY: int = int(os.getenv("REGIONAL_RULE_INITIAL_PRIORITY"))
PRIORITY_RANGE_FOR_REGIONAL: int = int(os.getenv("PRIORITY_RANGE_FOR_REGIONAL"))

# === Preprocess OWASP Rules ===
PRE_APPLIED_OWASP_RULE = {}
waf_rule_priority = WAF_RULE_PRIORITY
for owasp_rule in OWASP_RULE.split("+"):
    PRE_APPLIED_OWASP_RULE[owasp_rule] = waf_rule_priority
    waf_rule_priority = waf_rule_priority + 1
print(f"[GENERIC] Pre Defined OWASP RULE List are: {PRE_APPLIED_OWASP_RULE}")

# === Rule Range Initialization ===
RULE_RANGE = {}
starting_priority_range = REGIONAL_RULE_INITIAL_PRIORITY
ending_priority_range = PRIORITY_RANGE_FOR_REGIONAL
for file in SUPPORTED_FILE_NAMES:
    RULE_RANGE[file] = {
        "starting_priority_range": starting_priority_range,
        "ending_priority_range": ending_priority_range,
    }
    starting_priority_range = ending_priority_range + 1
    ending_priority_range = ending_priority_range + PRIORITY_RANGE_FOR_REGIONAL

print(f"[GENERIC] Policy Rule Range for each file is: {RULE_RANGE}")

# Bigtable Resource
PROJECT_ID = os.getenv("PROJECT_ID")
INSTANCE_ID = os.getenv("INSTANCE_ID")
T_ID = os.getenv("TABLE_ID")
COLUMN_FAMILY_ID = os.getenv("COLUMN_FAMILY_ID")


def get_current_timestamp():
    est_tz = pytz.timezone("US/Eastern")
    return datetime.now(est_tz).strftime("%Y-%m-%d %H:%M:%S.%f")


def retry_on_exception(
    max_retries=3, delay_seconds=2, audit_message="Operation failed"
):
    """
    Decorator for retrying a function call with exponential backoff and auditing failures.
    """

    def decorator(func):
        def wrapper(*args, **kwargs):
            for attempt in range(1, max_retries + 1):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    print(
                        f"[WARNING] {audit_message} (Attempt {attempt}/{max_retries}): {e}"
                    )
                    if attempt < max_retries:
                        time.sleep(
                            delay_seconds * (2 ** (attempt - 1))
                        )  # Exponential backoff
                    else:
                        print(
                            f"[CF2-ERROR] {audit_message} failed after {max_retries} attempts."
                        )
                        raise
            return None

        return wrapper

    return decorator


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

    @retry_on_exception(audit_message="Bigtable batch row insert operation")
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

        print(f"Batch of {len(rows)} rows inserted successfully.")

        return f"Batch of {len(rows)} rows inserted successfully."

    @retry_on_exception(audit_message="Bigtable single row insert operation")
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

        print(f"Row with key {row_key} inserted successfully.")

        return f"Row with key {row_key} inserted successfully."

    def create_bigtable_single_row_upsert_message(
        self, project_id, region, columns: dict
    ):
        """
        Encodes incoming parmas
        Upserts a single row with multiple columns.
        :param row_key: bytes
        :param column_data: dict of {column_name: value}
        """
        row_key = f"{project_id}#{region}".encode("utf-8")
        for col, value in columns.items():
            columns[col] = f"{value}".encode("utf-8")

        self.upsert_single_row(row_key, columns)


bt = BigtableUtility(PROJECT_ID, INSTANCE_ID, T_ID, COLUMN_FAMILY_ID)


# === Main Entry Function ===
def handle_policy_sync(project_id, region, address_groups):
    policies_timestamp = []
    action = True

    try:
        if not (project_id and region):
            print(
                f"[CF2-ERROR][Project-id: {project_id}, Region: {region}] Invalid input: Project ID or Region missing."
            )

            raise Exception(
                f"Invalid pair: Project ID or Region missing [Project-id: {project_id}, Region: {region}]."
            )

        policy_name = f"ca-policy-{region}"
        client = (
            compute_v1.SecurityPoliciesClient()
            if region == "global"
            else compute_v1.RegionSecurityPoliciesClient()
        )

        _response, action = create_cloud_armor_security_policy(
            client, project_id, policy_name, region
        )
        print(
            f"[INFO][Project-id: {project_id}, Region: {region}] Cloud Armor policy creation status retrieved."
        )

        policy_timestamp = []
        action_type = "create" if action else "update"

        for group in address_groups:
            try:
                access = group["rule"]
                group_name = group["group_name"]
                ip_address_list = group["ip_addresses"]
                invalid_data = group.get("invalid_ips", [])

                if invalid_data:
                    print(
                        f"[WARNING][Project-id: {project_id}, Region: {region}] group {group_name} has invalid ips {invalid_data}"
                    )

                if access not in ["allow", "deny"]:
                    raise Exception(
                        f"access type for rule is incorrect {access} it should be either allow or deny"
                    )

                update_or_create_ip_rules(
                    client,
                    project_id,
                    policy_name,
                    ip_address_list,
                    policy_timestamp,
                    access,
                    region,
                    group_name,
                )
                print(
                    f"[INFO][Project-id: {project_id}, Region: {region}] Rule added for group '{group_name}'. Response: SUCCESS"
                )
            except Exception as e:
                print(
                    f"[CF2-ERROR][Project-id: {project_id}, Region: {region}] Failed to create rule for group '{group_name}': {str(e)}"
                )
                policy_timestamp.append(
                    {
                        "policy_id": policy_name,
                        "project_id": project_id,
                        "region": region,
                        "comment": f"Error occured while creating rules for region {region} in project ID: {project_id}, error: {str(e)} ",
                        "latest_execution_status": "Failed",
                        "error_timestamp": get_current_timestamp(),
                    }
                )

                raise Exception(
                    f"Error occured while creating rules, error: {str(e)}"
                ) from e

        fetch_and_update_cloud_armor_waf_rule(
            client, project_id, policy_name, policy_timestamp, action_type, region
        )

        msg = {
            "latest_execution_status": "Success",
            "is_active": "True",
            "policy_id": policy_name,
            "failure_reason": "NA",
            "error_timestamp": "NA",
            "update_time": get_current_timestamp(),
        }

    except Exception as e:
        print(
            f"[CF2-ERROR][Project-id: {project_id}, Region: {region}] Failed to create Cloud Armor policy: {str(e)}"
        )
        policies_timestamp.append(
            {
                "project_id": project_id,
                "region": region,
                "comment": f"Error occurred, policy not created: {str(e)}",
                "latest_execution_status": "Failed",
                "error_timestamp": get_current_timestamp(),
            }
        )
        msg = {
            "policy_id": policy_name,
            "latest_execution_status": "Failed",
            "failure_reason": f"Error occurred while processing policy operation, Error :{str(e)}",
            "error_timestamp": get_current_timestamp(),
        }

    bt.create_bigtable_single_row_upsert_message(project_id, region, msg)

    return policy_timestamp


async def handle_policy_for_project_region(project_id, region, address_groups):
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(
        None, handle_policy_sync, project_id, region, address_groups
    )


async def run_batch(batch):
    tasks = [
        handle_policy_for_project_region(project_id, region, address_groups)
        for project_id, region, address_groups in batch
    ]
    return await asyncio.gather(*tasks)


def thread_runner(batch):
    return asyncio.run(run_batch(batch))


def create_cloud_armor_security_policies(request):
    print("[GENERIC] Incoming address")

    try:
        request_json = request.get_json(force=True)
    except Exception as e:
        print(f"Error parsing JSON: {e}")
        return f"Invalid JSON: {str(e)}", 400

    print(f"[GENERIC] Parsed JSON: {request_json}")
    print("[GENERIC] End of incoming address")

    try:
        projects = request_json.get("projects", [])
        address_groups = request_json.get("address_groups", [])
        policies_timestamp = {"policy_rules": []}

        project_region_tasks = [
            (project["project_id"], region, address_groups)
            for project in projects
            for region in project.get("regions", [])
        ]

        N = 15  # Number of threads
        batch_size = (len(project_region_tasks) + N - 1) // N
        batches = [
            project_region_tasks[i : i + batch_size]
            for i in range(0, len(project_region_tasks), batch_size)
        ]

        with ThreadPoolExecutor(max_workers=N) as executor:
            all_results = executor.map(thread_runner, batches)

        for batch_result in all_results:
            for result in batch_result:
                policies_timestamp["policy_rules"].append(result)
    except Exception as e:
        print(
            f"[CF2-ERROR][GENERIC] Error occured while processing main method for creating policies: {e}"
        )

        return "Error occured during policies upsert process"

    if policies_timestamp:
        print(f"[CF2-SUMMARY-ERROR] Policy Provisioning Error are, error: {policies_timestamp}")

    print(
        f"[GENERIC] Policies and rules upsert process completed for projects: {projects}"
    )

    return "Success"


def create_cloud_armor_security_policy(client, project_id, policy_name, region):
    policy_body = compute_v1.SecurityPolicy(
        name=policy_name, description=f"{region} - Application Layer Cloud Armor Policy"
    )
    if region.lower() == "global":
        policy_body = compute_v1.SecurityPolicy(
            name=policy_name,
            description="global - Application Layer Cloud Armor Policy",
        )
    # Check if the policy exists first
    try:
        policy = (
            client.get(project=project_id, region=region, security_policy=policy_name)
            if region != "global"
            else client.get(project=project_id, security_policy=policy_name)
        )
        print(
            f"[SKIP][Project-id: {project_id}, Region: {region}] Policy '{policy_name}' already exists."
        )
        return policy, False
    except Exception:
        try:
            print(
                f"[INFO][Project-id: {project_id}, Region: {region}] Creating new policy '{policy_name}'."
            )
            operation = (
                client.insert(
                    project=project_id, region=region, security_policy_resource=policy_body
                )
                if region != "global"
                else client.insert(project=project_id, security_policy_resource=policy_body)
            )
            operation.result()
            print(
                f"[SUCCESS][Project-id: {project_id}, Region: {region}] Policy '{policy_name}' created successfully."
            )
            return operation, True
        except Exception:
            raise


def fetch_and_update_cloud_armor_waf_rule(
    client, project_id, policy_name, policy_timestamp, action, region
):
    try:
        # Fetch the current Cloud Armor policy
        policy = (
            client.get(project=project_id, region=region, security_policy=policy_name)
            if region != "global"
            else client.get(project=project_id, security_policy=policy_name)
        )

        all_rules = policy.rules
        print(
            f"[INFO][Project-id: {project_id}, Region: {region}] Total existing rules: {len(all_rules)}"
        )

        # Filter out the rules within the specified priority range
        all_rules = [
            rule
            for rule in all_rules
            if rule.priority < WAF_RULE_PRIORITY or rule.priority > WAF_RULE_PRIORITY
        ]

        policy.rules = all_rules

        for pre_waf_rule, priority in PRE_APPLIED_OWASP_RULE.items():
            # Check if a WAF rule already exists
            print(
                f"[INFO][Project-id: {project_id}, Region: {region}] Adding new WAF rule {pre_waf_rule} with priority {priority} to policy '{policy_name}'"
            )

            # Define the WAF rule with appropriate configuration
            waf_rule = compute_v1.SecurityPolicyRule(
                priority=priority,
                match=compute_v1.SecurityPolicyRuleMatcher(
                    versioned_expr="EVALUATE_PRECONFIGURED_WAF",  # Ensure using the correct preconfigured WAF match
                    expr=compute_v1.Expr(
                        expression=pre_waf_rule  # The preconfigured WAF rule
                    ),
                ),
                preconfigured_waf_config=compute_v1.SecurityPolicyRulePreconfiguredWafConfig(),  # Empty if not needing customization
                action="deny(403)",  # Action to take on rule match (deny with 403)
                description="WAF rule for protection",
            )

            # Add the new rule to the policy
            policy.rules.append(waf_rule)

            if action.lower() != "create":
                break

        # Apply the updated policy with the new WAF rule
        if region == "global":
            operation = client.patch(
                project=project_id,
                security_policy=policy_name,
                security_policy_resource=policy,
            )
        else:
            operation = client.patch(
                project=project_id,
                region=region,
                security_policy=policy_name,
                security_policy_resource=policy,
            )

        print(
            f"[INFO][Project-id: {project_id}, Region: {region}] Waiting for WAF rule operation to complete..."
        )
        operation.result()

        print(
            f"[SUCCESS][Project-id: {project_id}, Region: {region}] New WAF rule applied with priority {priority} to policy '{policy_name}'"
        )

        return operation

    except Exception as e:
        print(
            f"[CF2-ERROR][Project-id: {project_id}, Region: {region}] Failed to fetch or apply WAF rule for policy '{policy_name}': {str(e)}"
        )
        comment = f"Error fetching or applying WAF rule because of {str(e)}"
        waf_rule_timestamp = {
            "project_id": project_id,
            "policy_id": policy_name,
            "region": region,
            "failure_reason": comment,
        }
        policy_timestamp.append(waf_rule_timestamp)

        raise Exception(comment) from e


def update_or_create_ip_rules(
    client,
    project_id,
    policy_name,
    ip_list,
    policy_timestamp,
    access,
    region,
    group_name,
):
    """
    Updates or creates IP range rules within the specified priority range.
    All rules are added or updated in a single patch. Old rules are removed in the same patch.
    """
    print(
        f"[INFO][Project-id: {project_id}][Region: {region}]  Adding '{access}' policy for IP list — group '{group_name}'"
    )
    # fetch priority range for address lists according to there groups
    priority_range_start = RULE_RANGE[group_name]["starting_priority_range"]
    priority_range_end = RULE_RANGE[group_name]["ending_priority_range"]

    try:
        print(
            f"[INFO][Project-id: {project_id}, Region: {region}] Starting update/create IP rules for group: {group_name}"
        )
        print(
            f"[INFO][Project-id: {project_id}, Region: {region}] Fetching policy '{policy_name}'"
        )

        access = access if access == "allow" else "deny(403)"

        if region == "global":
            policy = client.get(project=project_id, security_policy=policy_name)
        else:
            policy = client.get(
                project=project_id, region=region, security_policy=policy_name
            )

        fingerprint = policy.fingerprint
        print(
            f"[INFO][Project-id: {project_id}, Region: {region}] Policy fetched successfully"
        )

        ip_list = list(set(ip_list))
        print(
            f"[INFO][Project-id: {project_id}, Region: {region}] Unique IPs received: {len(ip_list)}"
        )

        if not ip_list:
            print(
                f"[INFO][Project-id: {project_id}, Region: {region}] Empty IP list provided — will remove old rules but no new rules will be added for group '{group_name}'"
            )

        all_rules = policy.rules
        print(
            f"[INFO][Project-id: {project_id}, Region: {region}] Existing rules count: {len(all_rules)}"
        )

        # Filter out the rules within the specified priority range
        all_rules = [
            rule
            for rule in all_rules
            if rule.priority < priority_range_start
            or rule.priority > priority_range_end
        ]

        print(
            f"[INFO][Project-id: {project_id}, Region: {region}] Cleaned old rules within priority range for group: {group_name}"
        )
        # Divide the IP list into chunks
        chunk_size = IP_CHUNK_SIZE
        ip_chunks = [
            ip_list[i : i + chunk_size] for i in range(0, len(ip_list), chunk_size)
        ]
        print(
            f"[INFO][Project-id: {project_id}, Region: {region}] IPs divided into {len(ip_chunks)} chunks (chunk size: {chunk_size})"
        )

        # Ensure we have enough priorities to create new rules
        if len(ip_chunks) > (priority_range_end - priority_range_start + 1):
            raise ValueError(
                f"Error: Not enough available priority slots for {group_name}. Needed: {len(ip_chunks)}"
            )

        # Process IP chunks and prepare new rules
        for index, chunk in enumerate(ip_chunks):
            rule_priority = priority_range_start + index
            rule_body = compute_v1.SecurityPolicyRule(
                description=f"Rule for {group_name} IP chunk {index + 1}",
                priority=rule_priority,
                match=compute_v1.SecurityPolicyRuleMatcher(
                    versioned_expr="SRC_IPS_V1",
                    config=compute_v1.SecurityPolicyRuleMatcherConfig(
                        src_ip_ranges=chunk
                    ),
                ),
                action=access,
            )
            all_rules.append(rule_body)

        print(
            f"[INFO][Project-id: {project_id}, Region: {region}] Submitting patch request to update rules for group '{group_name}'"
        )

        if region == "global":
            # Update the policy object with cleaned rules
            policy.rules = all_rules
            operation = client.patch(
                project=project_id,
                security_policy=policy_name,
                security_policy_resource=policy,
            )

        else:
            # Add or update the rules in the policy in a single patch operation
            patch_request = compute_v1.PatchRegionSecurityPolicyRequest(
                project=project_id,
                region=region,
                security_policy=policy_name,
                security_policy_resource=compute_v1.SecurityPolicy(
                    rules=all_rules, fingerprint=fingerprint
                ),
            )

            # Execute the patch operation
            operation = client.patch(patch_request)

        print(
            f"[INFO][Project-id: {project_id}, Region: {region}] upsert IP Rules operation submitted"
        )

        # Wait for the operation to complete
        operation.result()
        print(
            f"[SUCCESS][Project-id: {project_id}, Region: {region}] Policy updated for group '{group_name}'"
        )

        return operation

    except Exception as e:
        print(
            f"[CF2-ERROR][Project-id: {project_id}, Region: {region}] Error updating/creating IP rules for group '{group_name}': error {str(e)}"
        )
        policy_timestamp.append(
            {
                "project_id": project_id,
                "policy_id": policy_name,
                "region": region,
                "failure_reason": f"Error while upserting {group_name} regional rule because of {str(e)}",
            }
        )

        raise Exception(
            f"Error while upserting {group_name} regional rule because of {str(e)}"
        ) from e

