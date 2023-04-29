# ---- ECCO ----
import xml.etree.ElementTree as ET
import zipfile
from bs4 import BeautifulSoup
import os
import re
import pandas as pd
from lxml import etree

# Function to parse .hdr file and extract metadata


def extract_metadata(hdr_file):
    tree = etree.parse(hdr_file)
    root = tree.getroot()

    metadata = {}
    author_element = root.find(".//AUTHOR")
    if author_element is not None:
        author_text = author_element.text
        author_name = re.match(
            r"(.+?)(?:, \d{4}[-?]\d{4}|, d\. .*|\d{4}[-?]\d{4}|\d{4}|$)", author_text)
        metadata['AUTHOR'] = author_name.group(
            1).strip() if author_name else None
    else:
        metadata['AUTHOR'] = None
    metadata['TITLE'] = root.find(
        ".//TITLE").text if root.find(".//TITLE") is not None else None
    sourcedesc_date = root.find(".//SOURCEDESC/BIBLFULL/PUBLICATIONSTMT/DATE")
    metadata['DATE'] = sourcedesc_date.text if sourcedesc_date is not None else None
    metadata['DOCNO_IDNO'] = root.find(
        ".//IDNO[@TYPE='DocNo']").text if root.find(".//IDNO[@TYPE='DocNo']") is not None else None

    return metadata


# Function to extract text from the XML file


def extract_text_from_xml(xml_file_path):
    with open(xml_file_path, 'r', encoding='utf-8') as file:
        content = file.read()
        soup = BeautifulSoup(content, 'lxml')
        text = soup.get_text('\n')
        return text.strip()


# Define the base directories for headers, XML files, and txt outputs
headers_base_dir = "data/headers"
xml_base_dir = "data/XML"
txt_outputs_dir = "data/txt_outputs"

# Create the txt_outputs directory if it doesn't exist
os.makedirs(txt_outputs_dir, exist_ok=True)

all_metadata = []

# Iterate through the headers directory
for root, dirs, files in os.walk(headers_base_dir):
    for file in files:
        if file.endswith(".hdr"):
            hdr_file_path = os.path.join(root, file)
            metadata = extract_metadata(hdr_file_path)
            metadata['hdr_file_name'] = file
            all_metadata.append(metadata)

            # Find the corresponding XML file
            xml_file_name = file.replace('.hdr', '.xml')
            xml_file_path = None
            for xml_root, xml_dirs, xml_files in os.walk(xml_base_dir):
                if xml_file_name in xml_files:
                    xml_file_path = os.path.join(xml_root, xml_file_name)
                    break

            if xml_file_path:
                # Extract the text from the XML file
                text = extract_text_from_xml(xml_file_path)

                # Save the text to a .txt file in the txt_outputs directory
                txt_file_name = f"{metadata['DOCNO_IDNO']}.txt"
                txt_file_path = os.path.join(txt_outputs_dir, txt_file_name)
                with open(txt_file_path, 'w', encoding='utf-8') as txt_file:
                    txt_file.write(text)
            else:
                print(f"XML file not found for {file}")

# Create a DataFrame from the extracted metadata
metadata_df = pd.DataFrame(all_metadata)
metadata_df.to_csv('ECCO.csv')
print(metadata_df.head())

# # Read the CSV file into a DataFrame
# csv_file_path = "authornames.csv"
# csv_df = pd.read_csv(csv_file_path)

# # Convert the 'author' column of the CSV DataFrame into a set
# csv_authors = set(csv_df['author'])

# # Filter the metadata_df DataFrame based on the 'author' column of the CSV DataFrame
# filtered_metadata_df = metadata_df[metadata_df['AUTHOR'].isin(csv_authors)]

# # Print the filtered DataFrame
# print(filtered_metadata_df.head())
# filtered_metadata_df.to_csv('filtered_metadata.csv')

# --------------------------------------------- EEBO ---------------------------------------------

# --------------------------------------------- EEBO: Unzip all files ---------------------------------------------


# Define the input and output directories
# input_dir = "data/XML2/P4_XML_TCP"
input_dir = "data/XML3/P4_XML_TCP_Ph2"
# output_dir = "data/XML2"
output_dir = "data/XML3"

# Iterate through the input directory
for root, dirs, files in os.walk(input_dir):
    for file in files:
        if file.endswith(".zip"):
            # Construct the file paths
            zip_file_path = os.path.join(root, file)

            # Open the zip file
            with zipfile.ZipFile(zip_file_path, 'r') as zip_ref:
                # Extract all the XML files to the output directory
                for member in zip_ref.namelist():
                    if member.endswith('.xml'):
                        # Extract the XML file to a temporary location
                        temp_path = zip_ref.extract(member, output_dir)
                        # Move the extracted XML file to the output directory without preserving the folder structure
                        os.rename(temp_path, os.path.join(
                            output_dir, os.path.basename(member)))
                        # Remove any empty directories that were created during extraction
                        os.removedirs(os.path.dirname(temp_path))

# --------------------------------------------- EEBO: Convert Ph1/2 files into .txt ---------------------------------------------

# Set the path for the input and output folders
# xml_folder = 'data/XML2'
# txt_folder = 'data/txt_outputs2'
xml_folder = 'data/XML3'
txt_folder = 'data/txt_outputs3'


def write_element_text(element, f):
    """Recursively write the text content of an element and its children to a file."""
    if element.text is not None:
        f.write(element.text.strip() + '\n')
    for child in element:
        write_element_text(child, f)
    if element.tail is not None:
        f.write(element.tail.strip() + '\n')


# Parse each XML file in the input folder
counter = 0
for filename in os.listdir(xml_folder):
    counter += 1
    if filename.endswith('.xml'):
        # Remove the .P4 from the filename
        txt_filename = os.path.splitext(
            filename)[0].replace(".P4", "") + '.txt'
        txt_path = os.path.join(txt_folder, txt_filename)

        # Parse the XML file and write the contents to the TXT file
        xml_path = os.path.join(xml_folder, filename)
        tree = ET.parse(xml_path)
        root = tree.getroot()
        with open(txt_path, 'w') as f:
            for child in root:
                write_element_text(child, f)

    if counter % 1000 == 0:
        print(f"Processed {counter} files")


# --------------------------------------------- EEBO: Generate metadata for Ph1/Ph2 ---------------------------------------------

# Set the path for the input folder
# xml_folder = 'data/XML2'
xml_folder = 'data/XML3'

# file_path = 'data/headers2/eebo_phase1_IDs_and_dates.txt'
file_path = 'data/headers3/EEBO_Phase2_IDs_and_dates.txt'

# outfile = 'EEBO_1.csv'
outfile = 'EEBO_2.csv'


def clean_author_text(text):
    """Clean up the author text by removing specific patterns."""
    # Remove dates in the form of XXXX-XXXX, B.C., A.D., ? marks, "fl.", "ca.", "d.", and standalone years (XXXX)
    cleaned_text = re.sub(
        r'\b(\d{4}-\d{4}|B\.C\.|A\.D\.|,\s*fl\.\s*|,\s*d\.\s*|,\s*\?|,\s*b\.\s*\?|,\s*d\.\s*\?|\s*\d{4}-\?|\s*\d{4}\?|\s*\d{4}|-\s*\d{1,4}|\s*\?-|\s*\?|fl\.\s*\?|fl\.\s*\.|d\.\s*\?|d\.\s*\.|-\.|ca\.\s*\d{1,4})\b', '', text)

    # Remove extra spaces and trim the text
    cleaned_text = re.sub(r'\s+', ' ', cleaned_text).strip()

    # Remove the extra "." and "," at the end of the author names
    cleaned_text = re.sub(r'[,.]\s*$', '', cleaned_text)

    # use regular expressions to remove trailing punctuation
    cleaned_text = re.sub(
        r'[^\w\s]+(?=\s*$)|[^\w\s]*([^\w\s]+)\s*$', '', cleaned_text.strip())

    return cleaned_text


def get_element_value(root, tag):
    """Find the first occurrence of an element with the specified tag and return its text content."""
    element = root.find(tag)
    if element is not None:
        text = element.text.strip()
        if tag == './/AUTHOR':
            text = clean_author_text(text)
        return text
    return None


# Initialize an empty list to store the data
data = []

# Parse each XML file in the input folder
counter = 0
for filename in os.listdir(xml_folder):
    counter += 1
    if filename.endswith('.xml'):
        # Parse the XML file
        xml_path = os.path.join(xml_folder, filename)
        tree = ET.parse(xml_path)
        root = tree.getroot()

        # Extract the AUTHOR, TITLE, and DLPS values
        author = get_element_value(root, './/AUTHOR')
        title = get_element_value(root, './/TITLE')
        dlps = get_element_value(root, './/IDNO[@TYPE="DLPS"]')

        # Append the extracted values to the data list
        data.append({"author": author, "title": title,
                    "dlps": dlps, "filename": filename})

    if counter % 1000 == 0:
        print(f"Processed {counter} files")

# Create a DataFrame from the data list
df = pd.DataFrame(data)

# dates metadata
df_dates = pd.read_csv(file_path, sep='\t', header=None)
df_dates.columns = ['dlps', 'date']

# Merge the two DataFrames
df = pd.merge(df, df_dates, on='dlps', how='left')
# Save the DataFrame to a CSV file
df.to_csv(outfile, index=False)
