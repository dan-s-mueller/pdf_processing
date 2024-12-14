import os
from dotenv import load_dotenv,find_dotenv
load_dotenv(find_dotenv(), override=True)

from unstructured_ingest.v2.pipeline.pipeline import Pipeline
from unstructured_ingest.v2.interfaces import ProcessorConfig

from unstructured_ingest.v2.processes.connectors.fsspec.gcs import (
    GcsIndexerConfig,
    GcsDownloaderConfig,
    GcsConnectionConfig,
    GcsAccessConfig
)
from unstructured_ingest.v2.processes.connectors.local import (
    LocalUploaderConfig
)
from unstructured_ingest.v2.processes.partitioner import PartitionerConfig
from unstructured_ingest.v2.processes.chunker import ChunkerConfig

import json, base64, zlib
from typing import List, Dict, Any

def extract_orig_elements(orig_elements):
    # Extract the contents of an orig_elements field.
    decoded_orig_elements = base64.b64decode(orig_elements)
    decompressed_orig_elements = zlib.decompress(decoded_orig_elements)
    return decompressed_orig_elements.decode('utf-8')

def get_chunked_elements(input_json_file_path: str) -> List[Dict[str, Any]]:
    # Create a dictionary that will hold only
    # a transposed version of the returned elements. 
    # For instance, we just want to capture each element's ID,
    # the chunk's text, and the chunk's associated elements in context.
    orig_elements_dict: List[Dict[str, Any]] = []

    with open(input_json_file_path, 'r') as file:
        file_elements = json.load(file)

    for element in file_elements:
        # For each chunk that has an "orig_elements" field...
        if "orig_elements" in element["metadata"]:
            # ...get the chunk's associated elements in context...
            orig_elements = extract_orig_elements(element["metadata"]["orig_elements"])
            # ...and then transpose it and other associated fields into a separate dictionary.
            orig_elements_dict.append({
                "element_id": element["element_id"],
                "text": element["text"],
                "orig_elements": json.loads(orig_elements)
            })
    
    return orig_elements_dict

cloud_bucket_path='gs://processing-pdfs'
work_dir = './results'

if __name__ == "__main__":
    # Process documents with Unstructured.io
    Pipeline.from_configs(
        context=ProcessorConfig(disable_parallelism=True, work_dir=work_dir),
        indexer_config=GcsIndexerConfig(remote_url=cloud_bucket_path),
        downloader_config=GcsDownloaderConfig(download_dir=work_dir),
        source_connection_config=GcsConnectionConfig(
            access_config=GcsAccessConfig(
                service_account_key=os.getenv("GCS_SERVICE_ACCOUNT_KEY")
            )
        ),
        partitioner_config=PartitionerConfig(
            partition_by_api=True,
            api_key=os.getenv("UNSTRUCTURED_API_KEY"),
            partition_endpoint=os.getenv("UNSTRUCTURED_API_URL"),
            strategy="hi_res",
        ),
        chunker_config=ChunkerConfig(
            chunking_strategy="by_title",
            chunk_max_characters=2000,
            chunk_overlap=0,
        ),  
        uploader_config=LocalUploaderConfig()
    ).run()

    # Get the original document elements for each chunk.
    input_filepath = "./structured-output/1999_christiansen_reocr.pdf.json"
    output_filepath = "./structured-output/1999_christiansen_reocr-elements-only.json"

    orig_elements_dict = get_chunked_elements(input_json_file_path = input_filepath)

    # Convert the elements into a JSON object.
    orig_elements_json = json.dumps(orig_elements_dict, indent=2)

    # Write the JSON to a file.
    with open(output_filepath, "w") as file:
        file.write(orig_elements_json)
