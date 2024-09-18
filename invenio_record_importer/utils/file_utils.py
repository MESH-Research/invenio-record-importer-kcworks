import os
import fnmatch
import re


def sanitize_filenames(directory) -> list:
    changed = []
    for path, dirs, files in os.walk(directory):
        for filename in fnmatch.filter(files, "*[“”‘’]*"):
            file_path = os.path.join(path, filename)
            newname = re.sub(r"[“”‘’]", "", filename)
            new_file_path = os.path.join(path, newname)
            if file_path != new_file_path:
                os.rename(file_path, new_file_path)
                changed.append(new_file_path)
    return changed
