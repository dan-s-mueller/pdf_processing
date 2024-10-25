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

            # Generate a stripped PDF (remove images, keep text)
            stripped_file = os.path.join(data_dir, f"{doc_name}_stripped.pdf")
            ocrmypdf.ocr(
                input_file=local_file,
                output_file=stripped_file,
                output_type='pdf', 
                tesseract_timeout=0,                # Set to 0 for no timeout
                force_ocr=True,                     # Force OCR even if the PDF has existing text
                continue_on_soft_render_error=True  # Continue on soft render errors
            )
            print(f"Generated stripped PDF: {stripped_file}")

            # Upload processed file back to the original bucket
            bucket.blob(f"{doc_name_without_ext}_stripped.pdf").upload_from_filename(stripped_file)
            print(f"Uploaded {doc_name_without_ext}_stripped.pdf to {bucket_name}")

            # Apply OCR to the original PDF, generating a new PDF and text file
            reocr_file = os.path.join(data_dir, f"{doc_name}_reocr.pdf")
            reocr_txt = os.path.join(data_dir, f"{doc_name}_reocr.txt")
            ocrmypdf.ocr(
                input_file=stripped_file,
                output_file=reocr_file,
                output_type='pdf',   
                sidecar=reocr_txt,                   # Output OCR text to a sidecar .txt file
                continue_on_soft_render_error=True,  # Continue on soft render errors
                language='eng',                      # Specify the language(s) you expect in your documents
                clean=True,
                deskew=True,
                skip_text=False,
                optimize=1,  # Level 1 optimization (increase if needed, max is 3)
            )
            print(f"Generated OCR PDF: {reocr_file}")

            # Upload processed files back to the original bucket
            bucket.blob(f"{doc_name_without_ext}_reocr.pdf").upload_from_filename(reocr_file)
            bucket.blob(f"{doc_name_without_ext}_reocr.txt").upload_from_filename(reocr_txt)
            print(f"Uploaded {doc_name_without_ext}_reocr.pdf and {doc_name}_reocr.txt files to {bucket_name}")

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