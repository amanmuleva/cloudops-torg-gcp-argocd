import os
import yaml
import shutil
import sys
from deepdiff import DeepDiff

def create_the_template(folder_name, SUB_FOLDER, TEMPLATE_DIR, BASE_PATH):
    # This logic checks if the prefix of the fldr so we can determine where the fldr needs to be created
    # There is edge case which is used to check if the sufix has the more than 1 - to keep it as one string 
    part_name_of_folder = folder_name.split('-', 1)
    prefix_name = part_name_of_folder[0] 
    sufix_name = part_name_of_folder[1] if len(part_name_of_folder) > 1 else ""
    part_name_of_subfolder = SUB_FOLDER.split('-', 1)
    sub_prefix_name = part_name_of_subfolder[0]
    sub_sufix_name = part_name_of_subfolder[1] if len(part_name_of_subfolder) > 1 else ""
    print(os.getcwd())
    
    # This is used to create fldr file dir with root-app using helm-chart as the template file 
    try:
        if prefix_name == "fldr":
            os.chdir(BASE_PATH)
            os.mkdir(folder_name)
            os.chdir(folder_name)
            print(f"Folder named `{folder_name}` created successfully")
            os.mkdir("root-app")
            os.chdir("root-app")
            destination_folder = os.getcwd()
            print(f"Destination folder = {destination_folder}")
            # Copy Template files 
            shutil.copytree(TEMPLATE_DIR, destination_folder, dirs_exist_ok=True)
            os.chdir("templates")
            print(f"Templates path = {os.getcwd()}")

            with open("app-of-apps.yaml", 'r') as file:
                content = file.read()
                modified_content = content.replace("new_folder", sufix_name)

            with open("app-of-apps.yaml", 'w') as file:
                file.write(modified_content)

        elif prefix_name == "proj":
            subfolder_path = os.path.join(BASE_PATH, SUB_FOLDER)
            os.makedirs(subfolder_path, exist_ok=True)
            os.chdir(subfolder_path)
            os.mkdir(folder_name)
            print(f"Folder named `{folder_name}` created successfully")
            os.chdir(folder_name)
            print(f"The path to the {folder_name} = {os.getcwd()}")

            destination_folder = os.getcwd()
            print(f"destination folder = {destination_folder}")
            # Copy Template files 
            shutil.copytree(TEMPLATE_DIR, destination_folder, dirs_exist_ok=True)
            # Remove the values.yaml
            value_file_path = os.path.join(destination_folder, "values.yaml")
            os.remove(value_file_path)

            with open("dev.yaml", 'w') as file:
                file.write("env: dev\nconfigValues: |")
            with open("qa.yaml", 'w') as file:
                file.write("env: qa\nconfigValues: |")
            with open("prd.yaml", 'w') as file:
                file.write("env: prd\nconfigValues: |")

            os.chdir("templates")
            print(f"Templates path = {os.getcwd()}")

            os.rename("app-of-apps.yaml", "app.yaml")
            with open("app.yaml", 'w') as file:
                content = f'''apiVersion: argoproj.io/v1alpha1
kind: Application
metadata:
  name: proj-{sufix_name}-{{{{ .Values.env }}}}
  namespace: argocd
  finalizers:
  - resources-finalizer.argocd.argoproj.io
spec:
  project: {sub_sufix_name}-{{{{ .Values.env }}}}
  sources:
    - repoURL: 'git@github.com:vonage-atmos/cloudops-torg-gcp-central.git'
      path: crossplane-templates/
      targetRevision: main
      helm:
        values: |
{{{{ .Values.configValues | indent 10 }}}}
  destination:
    server: 'https://kubernetes.default.svc'
    namespace: {sub_sufix_name}-{{{{ .Values.env }}}}
  syncPolicy:
    syncOptions:
    - CreateNamespace=true
    - ApplyOutOfSyncOnly=true
    automated:
      prune: true
    retry:
      limit: 2
      backoff:
        duration: 5s
        factor: 2
        maxDuration: 3m0s'''
                file.write(content)

            aoayaml_path = os.path.join(BASE_PATH, SUB_FOLDER, "root-app", "templates")
            print(f"App of apps path = {aoayaml_path}")
            os.chdir(aoayaml_path)
            source_aoayaml = f"""\n# Source of app for the proj-{sufix_name}
    - repoURL: 'git@github.com:vonage-atmos/cloudops-torg-gcp-argocd.git'
      path: $TARGET_DIR/proj-{sufix_name}
      helm:
        valueFiles:
          - {{{{ .Values.env }}}}.yaml
      targetRevision: main"""

            with open("app-of-apps.yaml", 'a') as file:
                file.write(source_aoayaml)

            # change the file name in the Chart.yaml
            chart_yaml = os.path.join(BASE_PATH, SUB_FOLDER, folder_name)
            print(f"Chart.yaml path = {chart_yaml}")
            os.chdir(chart_yaml)
            with open("Chart.yaml", 'r') as file:
                content = file.read()
                modified_content = content.replace("root-app", sufix_name)

            with open("Chart.yaml", 'w') as file:
                file.write(modified_content)

    except FileExistsError:
        print(f"Folder `{folder_name}` already exists.")
    except FileNotFoundError:
        print(f"Directory path does not exist.")
    except PermissionError:
        print("Permission denied while creating the folder.")
    except Exception as e:
        print(f"Unexpected error: {e}")

def read_fldr_cf_yaml(YAML_PATH, BASE_PATH):
    with open(YAML_PATH, 'r') as file:
        yaml_content = yaml.safe_load(file)
    print(f"This is the Yaml Content {yaml_content}")
    actual_content = read_directory(BASE_PATH)
    print(f"This is the Actual Directory {actual_content}")
    
    yaml_content_diff, actual_content_diff = process_folder_differences(yaml_content, actual_content)

def extract_folder_structure(data, root_folder="fldr-cf"):
    folder_structure = {}
    
    # Get the root folder content
    for item in data[root_folder]:
        if isinstance(item, dict):
            for key, value in item.items():
                if key.startswith('fldr-'):  # This is a sub_folder
                    sub_folder = key
                    folder_structure[sub_folder] = []
                    
                    # Handle different value types
                    if value is None:
                        # Empty folder case 
                        continue
                    elif isinstance(value, list) and len(value) == 0:
                        # Empty list case
                        continue
                    elif isinstance(value, list):
                        # Process the list of project folders
                        for sub_item in value:
                            if isinstance(sub_item, dict):
                                for folder_name, folder_content in sub_item.items():
                                    # Only add the folder name (key) if it's not 'root-app'
                                    if folder_name != 'root-app':
                                        folder_structure[sub_folder].append(folder_name)
    
    return folder_structure

def get_folder_differences(yaml_content, actual_content):
    yaml_folders = extract_folder_structure(yaml_content)
    actual_folders = extract_folder_structure(actual_content)

    yaml_content_diff = {}
    actual_content_diff = {}

    # Include all sub_folders from YAML even if they don't exist in actual
    all_sub_folders = set(yaml_folders.keys()) | set(actual_folders.keys())

    for sub_folder in all_sub_folders:
        yaml_folders_set = set(yaml_folders.get(sub_folder, []))
        actual_folders_set = set(actual_folders.get(sub_folder, []))

        # Check if sub_folder itself needs to be created
        if sub_folder in yaml_folders and sub_folder not in actual_folders:
            # The entire sub_folder needs to be created
            yaml_content_diff[sub_folder] = [sub_folder]  # Add the folder itself
            if yaml_folders_set:  # If it also has project folders
                yaml_content_diff[sub_folder].extend(list(yaml_folders_set))
        else:
            # Sub_folder exists, check for project folders to create
            folders_to_create = yaml_folders_set - actual_folders_set
            if folders_to_create:
                yaml_content_diff[sub_folder] = list(folders_to_create)

        folders_to_delete = actual_folders_set - yaml_folders_set
        if folders_to_delete:
            actual_content_diff[sub_folder] = list(folders_to_delete)

    return yaml_content_diff, actual_content_diff

def process_folder_differences(yaml_content, actual_content):
    BASE_FOLDER = "fldr-cf"
    BASE_PATH = os.path.abspath(BASE_FOLDER)
    TEMPLATE_DIR = os.path.abspath("helm-chart")
    print(f"Template folder = {TEMPLATE_DIR}")
    yaml_content_diff, actual_content_diff = get_folder_differences(yaml_content, actual_content)

    print("Folders that need to be created:")
    for sub_folder, folder_names in yaml_content_diff.items():
        print(f"Sub_folder: {sub_folder}")
        for folder_name in folder_names:
            if folder_name.startswith('fldr-'):
                # This is a sub_folder that needs to be created
                create_the_template(folder_name, "", TEMPLATE_DIR, BASE_PATH)
                print(f"  - Create sub_folder: {folder_name}")
            else:
                # This is a project folder
                create_the_template(folder_name, sub_folder, TEMPLATE_DIR, BASE_PATH)
                print(f"  - Create project folder: {folder_name}")
        print()

    return yaml_content_diff, actual_content_diff

def read_directory(path):
    directory = {}

    for rootdir, dirnames, filenames in os.walk(path):
        dn = os.path.basename(rootdir)
        directory[dn] = []

        if dirnames:
            for d in dirnames:
                directory[dn].append(read_directory(path=os.path.join(path, d)))

            for f in filenames:
                directory[dn].append(f)
        else:
            directory[dn] = filenames

        return directory
    
    os.chdir(path)
    if len(sys.argv) == 1:
        p = os.getcwd()
    elif len(sys.argv) == 2:
        p = os.path.abspath(sys.argv[1])
    else:
        sys.stderr.write("Unexpected argument {}\n".format(sys.argv[2:]))

    try:
        with open("{}.yaml".format(os.path.basename(p)), "w") as f:
            try:
                yaml.dump(read_directory(path=p), f, default_flow_style=False)
                print("Dictionary written to {}.yaml".format(os.path.basename(p)))
            except Exception as e:
                print(e)
    except Exception as e:
        print(e)

def main():
    BASE_FOLDER = "fldr-cf"
    BASE_PATH = os.path.abspath(BASE_FOLDER)
    TEMPLATE_DIR = os.path.abspath("helm-chart")
    print(f"Template folder = {TEMPLATE_DIR}")

    # Define the absolute path of YAML File  
    YAML_PATH = os.path.join('fldr-cf.yaml')
    
    read_fldr_cf_yaml(YAML_PATH, BASE_PATH)

if __name__ == "__main__":
    main()