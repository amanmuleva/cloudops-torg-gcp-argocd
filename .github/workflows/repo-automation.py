import os
import yaml
import sys
from pathlib import Path
import shutil

# The script is located in .github/workflows, we need to go up two levels to reach the root of the repository
script_dir = Path(__file__).parent

# The root of the repository is two levels up from the script's directory
workspace_dir = script_dir.parent.parent

# Construct the path to the fldr-cf directory and the fldr-cf.yaml file
root_dir = workspace_dir / "fldr-cf"
fldr_cf_yaml_path = workspace_dir / "fldr-cf.yaml"

print("This is the actual Directory:")

# Store actual directory structure as a dictionary
actual_dirs = {}
for dir in os.listdir(root_dir):
    dir_path = root_dir / dir
    if dir_path.is_dir():
        list_of_files = os.listdir(dir_path)
        actual_dirs[dir] = sorted(list_of_files)
        print(f"List of files in {dir}: {sorted([*list_of_files])}")

# Read the contents of fldr-cf.yaml 
yaml_dirs = {}
with open(fldr_cf_yaml_path, "r") as f:
    print("\nContents of fldr-cf.yaml:")
    try:
        yaml_content = yaml.safe_load(f)
        for list_of_files in yaml_content["fldr-cf"]:
            for folder_name, project_list in list_of_files.items():
                yaml_dirs[folder_name] = sorted(project_list)
                print(f"List of files in {folder_name}: {sorted([*project_list])}")
    except yaml.YAMLError as exc:
        print(exc)

# Now that we have the list of actual directories and the list of directories in fldr-cf.yaml, we can compare them
def compare_directories(actual_dirs, yaml_dirs):
    mismatches = {}
    missing_folders = []
    
    for folder_name in set(list(actual_dirs.keys()) + list(yaml_dirs.keys())):
        if folder_name in yaml_dirs and folder_name not in actual_dirs:
            print(f"\n{folder_name} : MISSING in actual directory")
            missing_folders.append(folder_name)
            continue

        actual = set(actual_dirs.get(folder_name, []))
        yaml = set(yaml_dirs.get(folder_name, []))
        
        if actual == yaml:
            print(f"\n{folder_name}: MATCH")
        else:
            print(f"\n{folder_name}: MISMATCH")
            
            missing_in_actual = yaml - actual
            
            if missing_in_actual:
                print(f"Missing in actual directory: {missing_in_actual}")
                mismatches[folder_name] = missing_in_actual
    
    return mismatches, missing_folders

def create_missing_directories(missing_folders):
    base_path = workspace_dir / "fldr-cf"
    template_path = workspace_dir / "helm-chart"

    for folder in missing_folders:
        new_folder_path = base_path / folder
        folder_parts = folder.split('-', 1)
        folder_suffix = folder_parts[1] if len(folder_parts) > 1 else ""

        print(f"Creating missing folder: {new_folder_path}")
        new_folder_path.mkdir(parents=True, exist_ok=True)

        # Create root-app and copy template
        root_app_path = new_folder_path / "root-app"
        root_app_path.mkdir(parents=True, exist_ok=True)
        shutil.copytree(str(template_path), str(root_app_path), dirs_exist_ok=True)

        # Modify app-of-apps.yaml — replace placeholder with folder suffix
        aoa_yaml_path = root_app_path / "templates" / "app-of-apps.yaml"
        if aoa_yaml_path.exists():
            with open(aoa_yaml_path, 'r') as f:
                content = f.read()
            content = content.replace("new_folder", folder_suffix)
            with open(aoa_yaml_path, 'w') as f:
                f.write(content)

        print(f"Folder {new_folder_path} with root-app created successfully.")


def repo_restructure(folder, missing_projects):
    base_path = workspace_dir / "fldr-cf" / folder
    template_path = workspace_dir / "helm-chart"
    print(f"\nCreating missing directories in {base_path} for projects: {missing_projects}")
    print(f"Using template from {template_path}")

    fldr_parts = folder.split('-', 1)
    fldr_suffix_name = fldr_parts[1] if len(fldr_parts) > 1 else ""
    print(f"fldr suffix name: {fldr_suffix_name}")

    for project in missing_projects:
        proj_parts = project.split('-', 1)
        proj_suffix_name = proj_parts[1] if len(proj_parts) > 1 else ""
        print(f"Project suffix name: {proj_suffix_name}")

        new_project_path = base_path / project
        new_project_path.mkdir(parents=True, exist_ok=True)

        # Copy template files into the project directory
        shutil.copytree(str(template_path), str(new_project_path), dirs_exist_ok=True)

        # Remove values.yaml (not needed for project folders)
        values_file = new_project_path / "values.yaml"
        if values_file.exists():
            values_file.unlink()

        # Create environment-specific value files
        for env in ["dev", "qa", "prd"]:
            env_file = new_project_path / f"{env}.yaml"
            with open(env_file, 'w') as f:
                f.write(f"env: {env}\nconfigValues: |")

        # Rename app-of-apps.yaml to app.yaml and write project-specific content
        templates_dir = new_project_path / "templates"
        aoa_yaml_path = templates_dir / "app-of-apps.yaml"
        app_yaml_path = templates_dir / "app.yaml"

        if aoa_yaml_path.exists():
            aoa_yaml_path.rename(app_yaml_path)

        with open(app_yaml_path, 'w') as f:
            content = f'''apiVersion: argoproj.io/v1alpha1
kind: Application
metadata:
  name: proj-{proj_suffix_name}-{{{{ .Values.env }}}}
  namespace: argocd
  finalizers:
  - resources-finalizer.argocd.argoproj.io
spec:
  project: {fldr_suffix_name}-{{{{ .Values.env }}}}
  sources:
    - repoURL: 'git@github.com:vonage-atmos/cloudops-torg-gcp-central.git'
      path: crossplane-templates/
      targetRevision: main
      helm:
        values: |
{{{{ .Values.configValues | indent 10 }}}}
  destination:
    server: 'https://kubernetes.default.svc'
    namespace: {fldr_suffix_name}-{{{{ .Values.env }}}}
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
            f.write(content)

        # Append source block to parent folder's root-app app-of-apps.yaml
        root_app_aoa = base_path / "root-app" / "templates" / "app-of-apps.yaml"
        if root_app_aoa.exists():
            source_block = f"""\n# Source of app for the proj-{proj_suffix_name}
    - repoURL: 'git@github.com:vonage-atmos/cloudops-torg-gcp-argocd.git'
      path: fldr-cf/{folder}/proj-{proj_suffix_name}
      helm:
        valueFiles:
          - {{{{ .Values.env }}}}.yaml
      targetRevision: main"""
            with open(root_app_aoa, 'a') as f:
                f.write(source_block)
            print(f"Appended source block for {project} to {root_app_aoa}")

        # Update Chart.yaml name
        chart_yaml_path = new_project_path / "Chart.yaml"
        if chart_yaml_path.exists():
            with open(chart_yaml_path, 'r') as f:
                content = f.read()
            content = content.replace("root", proj_suffix_name)
            with open(chart_yaml_path, 'w') as f:
                f.write(content)

        print(f"Created project structure for {project} under {folder}")
        
def main():
    mismatches, missing_folders = compare_directories(actual_dirs, yaml_dirs)

    # First, create any entirely missing folders (fldr-* with root-app)
    if missing_folders:
        create_missing_directories(missing_folders)

        # After creating the folders, add them to actual_dirs with only 'root-app'
        # so the second comparison picks up the missing projects inside them
        for folder in missing_folders:
            actual_dirs[folder] = ['root-app']

        # Re-compare to detect missing projects inside newly created folders
        mismatches, _ = compare_directories(actual_dirs, yaml_dirs)

    if mismatches:
        print("\nSummary of all mismatches:")
        for folder, missing_projects in mismatches.items():
            repo_restructure(folder, missing_projects)
    else:
        print("\nAll directories match!")

if __name__ == "__main__":
    main()
