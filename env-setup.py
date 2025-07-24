import os
import sys
import yaml

def dir_to_dict(path):
    directory = {}
    dn = os.path.basename(path)
    directory[dn] = []

    try:
        for d in os.listdir(path):
            full_path = os.path.join(path, d)
            if os.path.isdir(full_path) and d not in ['root-app', 'templates', 'ca-gcf']:
                directory[dn].append(dir_to_dict(full_path))

        if not directory[dn]:
            del directory[dn]
    except Exception as e:
        print(f"Error reading {path}: {e}")

    return directory

if len(sys.argv) == 1:
    p = os.getcwd()
elif len(sys.argv) == 2:
    p = os.path.abspath(sys.argv[1])
else:
    sys.stderr.write("Unexpected argument {}\n".format(sys.argv[2:]))

try:
    with open(f"{os.path.basename(p)}.yaml", "w") as f:
        try:
            yaml.dump(dir_to_dict(path=p), f, default_flow_style=False)
            print(f"Directory structure written to {os.path.basename(p)}.yaml")
        except Exception as e:
            print(e)
except Exception as e:
    print(e)
