from google.cloud import documentai_v1 as documentai
from google.cloud import storage
from google.cloud.documentai_toolbox import gcs_utilities, document
import os
import fitz
from PIL import Image
import ocrmypdf
import json

# TODO this doesn't quite work. The batches are processed in temp folders, but the json is appended and it's confusing to understand how it's processed. Stick with the single file processing for now.

def remove_text_by_rasterizing(input_pdf, output_pdf):
    """
    Removes all text content from a PDF by rasterizing it into images
    and recreating the PDF with just the images.
    """
    doc = fitz.open(input_pdf)
    image_pages = []

    for page_number in range(len(doc)):
        # Render each page as an image
        page = doc[page_number]
        pix = page.get_pixmap(dpi=150)  # High resolution for better quality
        image = Image.frombytes("RGB", [pix.width, pix.height], pix.samples)
        image_pages.append(image)

    # Save the images as a new PDF
    if image_pages:
        image_pages[0].save(output_pdf, save_all=True, append_images=image_pages[1:])
    print(f"PDF without text saved to: {output_pdf}")

def create_batches_and_process(
    project_id: str,
    location: str,
    processor_id: str,
    gcs_bucket_name: str,
    gcs_prefix: str,
    batch_size: int = 2
) -> None:
    # Create batches of documents for processing
    batches = gcs_utilities.create_batches(
        gcs_bucket_name=gcs_bucket_name, gcs_prefix=gcs_prefix, batch_size=batch_size
    )

    print(f"{len(batches)} batch(es) created.")
    client = documentai.DocumentProcessorServiceClient()
    processor_name = f"projects/{project_id}/locations/{location}/processors/{processor_id}"

    for i, batch in enumerate(batches, 1):
        print(f"Processing batch {i} of {len(batches)}")
        print(f"{len(batch.gcs_documents.documents)} files in batch.")
        print(batch.gcs_documents.documents)

        # Create a batch processing request
        request = documentai.BatchProcessRequest(
            name=processor_name,
            input_documents=batch,
            document_output_config=documentai.DocumentOutputConfig(
                gcs_output_config=documentai.DocumentOutputConfig.GcsOutputConfig(
                    gcs_uri=f"gs://{gcs_bucket_name}/output/"
                )
            )
        )

        # Send the batch request
        operation = client.batch_process_documents(request=request)

        # Wait for the operation to complete
        print("Waiting for operation to complete...")
        operation.result(timeout=300)

        # Process the results
        print(f"Batch {i} processed successfully. Output stored in GCS.")

    # Download processed documents from GCS
    download_processed_documents(gcs_bucket_name, "output/")

def download_processed_documents(bucket_name, prefix):
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blobs = bucket.list_blobs(prefix=prefix)

    data_folder = os.path.join(os.path.dirname(__file__), 'data')
    os.makedirs(data_folder, exist_ok=True)

    for blob in blobs:
        if blob.name.endswith('.json'):
            destination_file_name = os.path.join(data_folder, os.path.basename(blob.name))
            blob.download_to_filename(destination_file_name)
            print(f"Downloaded {blob.name} to {destination_file_name}")

            # Download the corresponding original PDF
            base_name = os.path.splitext(os.path.basename(blob.name))[0]
            download_pdf_from_gcs(bucket_name, base_name + '.pdf', data_folder)

            # Process the downloaded JSON to reapply OCR
            reapply_ocr_to_pdf(base_name, data_folder)

            # Upload the modified PDF back to GCS
            output_pdf_path = os.path.join(data_folder, base_name + "_reocr.pdf")
            upload_to_gcs(output_pdf_path, f"gs://{bucket_name}/processed/{os.path.basename(output_pdf_path)}")

def download_pdf_from_gcs(bucket_name, pdf_name, data_folder):
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(pdf_name)
    blob.download_to_filename(os.path.join(data_folder, pdf_name))
    print(f"Downloaded original PDF {pdf_name} to {os.path.join(data_folder, pdf_name)}")

def reapply_ocr_to_pdf(base_name, data_folder):
    # Load the JSON and extract the necessary information
    with open(os.path.join(data_folder, base_name + '.json'), 'r') as f:
        document_data = json.load(f)

    pdf_without_text_path = os.path.join(data_folder, base_name + "_pdf_without_text.pdf")
    output_pdf_path = os.path.join(data_folder, base_name + "_output.pdf")

    # Remove the existing OCR text layer
    remove_text_by_rasterizing(os.path.join(data_folder, base_name + '.pdf'), pdf_without_text_path)

    # Use the extracted data to reapply OCR
    hocr_string = convert_json_to_hocr(base_name, data_folder)
    convert_hocr_to_pdf(hocr_string, pdf_without_text_path, output_pdf_path)

def convert_json_to_hocr(base_name, data_folder):
    # Load the JSON data
    with open(os.path.join(data_folder, base_name + '.json'), 'r') as f:
        document_data = json.load(f)

    # Convert the JSON data to a Document object
    wrapped_document = document.Document.from_dict(document_data)

    # Export the Document object to hOCR format
    hocr_string = wrapped_document.export_hocr_str(title=os.path.join(data_folder, base_name + '.pdf'))
    with open(os.path.join(data_folder, base_name + '.hocr'), 'w', encoding='utf-8') as f:
        f.write(hocr_string)
    print(f"hOCR file saved to: {os.path.join(data_folder, base_name + '.hocr')}")

    return hocr_string

def convert_hocr_to_pdf(hocr_path, image_pdf_path, output_pdf_path):
    """Converts hOCR file to searchable PDF using ocrmypdf."""
    try:
        ocrmypdf.ocr(
            input_file=image_pdf_path,
            output_file=output_pdf_path,
            sidecar=hocr_path,
            skip_text=True,
            optimize=3
        )
        print(f"PDF with searchable text created at: {output_pdf_path}")
    except ocrmypdf.exceptions.PdfMergeFailedError as e:
        print(f"Error converting hOCR to PDF: {e}")

def upload_to_gcs(file_path, gcs_uri):
    storage_client = storage.Client()
    bucket_name, blob_name = gcs_uri.replace("gs://", "").split("/", 1)
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    blob.upload_from_filename(file_path)
    print(f"Uploaded {file_path} to {gcs_uri}")

# Example usage
project_id = "ai-aerospace"
location = "us"
processor_id = "baa26d1093093c7e"
gcs_bucket_name = "test-pdfs-small"
gcs_prefix = ""

create_batches_and_process(project_id, location, processor_id, gcs_bucket_name, gcs_prefix)
