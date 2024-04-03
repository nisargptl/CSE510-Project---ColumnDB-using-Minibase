import os
import fileinput

# Define the new JDKPATH
new_jdkpath = "/new/path/to/jdk"

# Walk through the src directory
for dirpath, dirnames, filenames in os.walk("src"):
    for filename in filenames:
        # Check if the file is a Makefile
        if filename == "Makefile":
            makefile_path = os.path.join(dirpath, filename)
            # Open the Makefile and replace the JDKPATH
            with fileinput.FileInput(makefile_path, inplace=True) as file:
                for line in file:
                    if line.strip().startswith("JDKPATH"):
                        print(f"JDKPATH = {new_jdkpath}")
                    else:
                        print(line, end="")
