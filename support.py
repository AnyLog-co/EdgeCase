import os
import shutil

def create_dir(dir_name:str):
    full_path = os.path.expanduser(os.path.expandvars(dir_name))
    if not os.path.isdir(full_path):
        try:
            os.makedirs(full_path)
        except Exception as err:
            raise Exception(f"Failed to create directory {dir_name} (Error: {err}")

def write_file(file_path:str, content):
    full_path = os.path.expanduser(os.path.expandvars(file_path))
    try:
        with open(full_path, 'w') as f:
            f.write(content)
    except Exception as err:
        raise Exception(f"Failed to write content into {file_path} (Error: {err})")

def copy_file(source_file:str, dest_file:str):
    full_source = os.path.expandvars(os.path.expanduser(source_file))
    full_dest = os.path.expandvars(os.path.expanduser(dest_file))

    if not os.path.isfile(full_source):
        raise FileNotFoundError(f"Failed to locate {source_file}")
    elif not os.path.isfile(full_dest):
        try:
            shutil.copy(full_source, full_dest)
        except (Exception, shutil.ExecError) as err:
            raise Exception(f"Failed to copy {source_file} -> {dest_file} (Error: {err})")

def read_file(content_file:str):
    full_path = os.path.expanduser(os.path.expandvars(content_file))
    if not os.path.isfile(full_path):
        FileNotFoundError(f"Failed to locate {full_path}")

    try:
        with open(full_path, 'r', encoding="utf-8") as f:
            return f.read()
    except Exception as err:
        raise Exception(f"Failed to read content in {content_file} (error: {err})")
