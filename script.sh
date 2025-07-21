#!/bin/bash

# Define the base directory
BASE_DIR="fldr-cf"

# # Check if the base directory exists
# if [ ! -d "$BASE_DIR" ]; then
#     echo "Base directory $BASE_DIR does not exist. Creating it now."
#     mkdir -p "$BASE_DIR"
# fi

# List existing folders starting with 'fldr-' and prompt the user for action
echo "Existing folders in $BASE_DIR folder:"
existing_folders=($(ls -d "$BASE_DIR"/fldr-* | xargs -n 1 basename))
for i in "${!existing_folders[@]}"; do
    echo "$((i+1)). ${existing_folders[$i]}"
done

echo
read -p "Do you want to use an existing folder (Enter 'e') or create a new one (Enter 'n')? " choice

# Variable to store the target directory
TARGET_DIR=""

if [ "$choice" = "e" ]; then
    read -p "Enter the number of the existing folder you want to use, from the above list: " folder_number
    if [[ $folder_number -gt 0 && $folder_number -le ${#existing_folders[@]} ]]; then
        TARGET_DIR="$BASE_DIR/${existing_folders[$((folder_number-1))]}"
        FOLDER_NAME="${existing_folders[$((folder_number-1))]#fldr-}"
        # echo "Folder name = $FOLDER_NAME"
    else
        echo "Invalid folder number. Exiting!"
        exit 1
    fi
elif [ "$choice" = "n" ]; then
    read -p "Enter the name of the new folder you want to create (should start with 'fldr-'): " new_folder
    if [[ $new_folder != fldr-* ]]; then
        echo "The new folder name must start with 'fldr-'. Exiting!"
        exit 1
    fi
    TARGET_DIR="$BASE_DIR/$new_folder"
    mkdir -p "$TARGET_DIR"
    echo -e "\nNew folder $TARGET_DIR created."

    # Create 'root-app' folder and specified files/subfolders in the target directory
    ROOT_APP_DIR="$TARGET_DIR/root-app"
    TEMPLATES_DIR="$ROOT_APP_DIR/templates"

    mkdir -p "$ROOT_APP_DIR"
    mkdir -p "$TEMPLATES_DIR"

    # touch "$ROOT_APP_DIR/.helmignore"
    # touch "$ROOT_APP_DIR/Chart.yaml"
    # touch "$ROOT_APP_DIR/values.yaml"
    # touch "$TEMPLATES_DIR/app-of-apps.yaml"

    cp -r helm-chart/. $ROOT_APP_DIR/
    FOLDER_NAME=${new_folder#fldr-}
    sed -i '' "s/new_folder/${FOLDER_NAME}/g" "$TEMPLATES_DIR/app-of-apps.yaml"


    echo -e "\nCreated .helmignore, Chart.yaml, values.yaml, and templates/app-of-apps.yaml in $ROOT_APP_DIR"
else
    echo "Invalid choice. Exiting!"
    exit 1
fi

echo " "

# Create a proj-* folder inside the selected fldr-* folder
read -p "Enter the name for the project folder (suffix after 'proj-'): " proj_suffix
if [[ $proj_suffix = proj-* ]]; then
  echo "Only new project name (suffix after 'proj-') is enough. No need to start with 'proj-'. Exiting!"
  exit 1
fi

PROJ_DIR="$TARGET_DIR/proj-$proj_suffix"

if [ -d "$PROJ_DIR" ]; then
    echo "Project folder $PROJ_DIR already exists. Exiting!"
    exit 1
else
    mkdir -p "$PROJ_DIR"
    echo -e "\nNew project folder $PROJ_DIR created."
fi

# Create files and folders inside the proj-* folder
TEMPLATES_DIR="$PROJ_DIR/templates"

mkdir -p "$TEMPLATES_DIR"

echo -e "env: dev\nconfigValues: |" > "$PROJ_DIR/dev.yaml"
echo -e "env: qa\nconfigValues: |" > "$PROJ_DIR/qa.yaml"
echo -e "env: prd\nconfigValues: |" > "$PROJ_DIR/prd.yaml"

cp "$TARGET_DIR/root-app/.helmignore" "$PROJ_DIR/.helmignore"
cp "$TARGET_DIR/root-app/Chart.yaml" "$PROJ_DIR/Chart.yaml"
sed  -i '' "s/root-app/${proj_suffix}-app/g" "$PROJ_DIR/Chart.yaml"

cat > "$TEMPLATES_DIR/app.yaml" <<EOL
apiVersion: argoproj.io/v1alpha1
kind: Application
metadata:
  name: proj-${proj_suffix}-{{ .Values.env }}
  namespace: argocd
  finalizers:
  - resources-finalizer.argocd.argoproj.io
spec:
  project: ${FOLDER_NAME}-{{ .Values.env }}
  sources:
    - repoURL: 'git@github.com:vonage-atmos/cloudops-torg-gcp-central.git'
      path: crossplane-templates/
      targetRevision: main
      helm:
        values: |
{{ .Values.configValues | indent 10 }}
  destination:
    server: 'https://kubernetes.default.svc'
    namespace: ${FOLDER_NAME}-{{ .Values.env }}
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
        maxDuration: 3m0s
EOL


echo -e "\nCreated .helmignore, Chart.yaml, dev.yaml, qa.yaml, prd.yaml, and templates/app.yaml in $PROJ_DIR"


# Append to app-of-apps.yaml
APP_OF_APPS_FILE="$TARGET_DIR/root-app/templates/app-of-apps.yaml"
if [ -f "$APP_OF_APPS_FILE" ]; then
    cat >> "$APP_OF_APPS_FILE" <<EOL
    # Source of app for the proj-$proj_suffix
    - repoURL: 'git@github.com:vonage-atmos/cloudops-torg-gcp-argocd.git'
      path: $TARGET_DIR/proj-$proj_suffix
      helm:
        valueFiles:
          - {{ .Values.env }}.yaml
      targetRevision: main
EOL
    echo -e "\nAppended project details to $APP_OF_APPS_FILE"
fi

echo -e "\nAll specified folders and files have been created in $PROJ_DIR."
