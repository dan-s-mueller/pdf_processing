import os
import sys
import ocrmypdf
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
        blobs = [blob for blob in blobs if blob.name.endswith('.pdf') and 
                 not ('_stripped.pdf' in blob.name or '_reocr.pdf' in blob.name)]

    # Print the list of files to be processed
    print("Files to be processed:")
    for blob in blobs:
        print(f"- {blob.name}")

    # Create the /data directory if it doesn't exist
    data_dir = './data'
    os.makedirs(data_dir, exist_ok=True)

    for blob in blobs:
        doc_name = blob.name
        doc_name_without_ext = doc_name[:-4]  # Remove the '.pdf' extension
        print(f"Processing {doc_name}")
        try:
            # Download the PDF file from GCS to the /src/data directory
            local_file = os.path.join(data_dir, doc_name)
            blob.download_to_filename(local_file)
            print(f"Downloaded {doc_name} to {local_file}")

            # Remove the '.pdf' extension from doc_name, process by reocr'ing it
            doc_name_base = os.path.splitext(doc_name)[0]
            reocr_file = os.path.join(data_dir, f"{doc_name_base}_reocr.pdf")
            ocrmypdf.ocr(
                input_file=local_file,
                output_file=reocr_file,
                output_type='pdf',   
                continue_on_soft_render_error=True,  # Continue on soft render errors
                language='eng',                      # Specify the language(s) you expect in your documents
                force_ocr=True,
                clean=True,
                # deskew=True,
                skip_text=False,
                optimize=3,  # Maximum optimization for file size reduction
                jpeg_quality=85,  # Adjust JPEG quality for a balance between size and quality
                png_quality=85,  # Adjust PNG quality similarly
            )
            print(f"Generated OCR PDF: {reocr_file}")

            # Upload processed files back to the original bucket
            bucket.blob(f"{doc_name_base}_reocr.pdf").upload_from_filename(reocr_file)
            print(f"Processed {doc_name_base}_reocr.pdf")

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

    # Check if all provided file arguments end with '.pdf'
    if specific_files:
        non_pdf_files = [file for file in specific_files if not file.lower().endswith('.pdf')]
        if non_pdf_files:
            print(f"Error: The following files do not have a .pdf extension: {', '.join(non_pdf_files)}")
            sys.exit(1)

    # If no specific files are provided, set to None to process all files in the bucket
    if not specific_files:
        specific_files = None
    
    # Start processing PDFs
    process_pdfs(bucket_name, specific_files)