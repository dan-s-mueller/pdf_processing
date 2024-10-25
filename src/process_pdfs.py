import os
import sys
import tempfile
from google.cloud import storage

def process_pdfs(bucket_name, specific_files=None):
    # Initialize Google Cloud Storage client
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)

    # Determine which files to process
    if specific_files:
        blobs = [bucket.blob(file) for file in specific_files if file.endswith('.pdf')]
    else:
        blobs = list(bucket.list_blobs(prefix='', delimiter='/'))
        blobs = [blob for blob in blobs if blob.name.endswith('.pdf')]

    # Create a temporary directory to store files during processing
    with tempfile.TemporaryDirectory() as temp_dir:
        for blob in blobs:
            doc_name = blob.name
            print(f"Processing {doc_name}")
            try:
                # Download the PDF file from GCS
                local_file = os.path.join(temp_dir, doc_name)
                blob.download_to_filename(local_file)

                # Generate a stripped PDF (remove images, keep text)
                stripped_file = os.path.join(temp_dir, f"{doc_name}_stripped.pdf")
                os.system(f'ocrmypdf --tesseract-timeout 0 --continue-on-soft-render-error --force-ocr "{local_file}" "{stripped_file}"')
                
                # Apply OCR to the original PDF, generating a new PDF and text file
                reocr_file = os.path.join(temp_dir, f"{doc_name}_reocr.pdf")
                reocr_txt = os.path.join(temp_dir, f"{doc_name}_reocr.txt")
                os.system(f'ocrmypdf --sidecar "{reocr_txt}" --continue-on-soft-render-error --redo-ocr "{local_file}" "{reocr_file}"')

                # Upload processed files back to the original bucket
                bucket.blob(f"{doc_name}_stripped.pdf").upload_from_filename(stripped_file)
                bucket.blob(f"{doc_name}_reocr.pdf").upload_from_filename(reocr_file)
                bucket.blob(f"{doc_name}_reocr.txt").upload_from_filename(reocr_txt)

            except Exception as e:
                print(f'Error processing {doc_name}: {str(e)}')

if __name__ == "__main__":
    # Check for correct usage
    if len(sys.argv) < 2:
        print("Usage: python process_pdfs.py <bucket_name> [file1.pdf file2.pdf ...]")
        sys.exit(1)
    
    # Get bucket name from command line arguments
    bucket_name = sys.argv[1]
    
    # Get specific files to process (if any)
    specific_files = sys.argv[2:] if len(sys.argv) > 2 else None

    # If no specific files are provided, set to None to process all files in the bucket
    if not specific_files:
        specific_files = None
    
    # Start processing PDFs
    process_pdfs(bucket_name, specific_files)