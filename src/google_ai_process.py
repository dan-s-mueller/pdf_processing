from google.cloud import documentai_v1 as documentai
from google.cloud.documentai_toolbox import document
import fitz
import os
from tqdm import tqdm
from PIL import Image
import ocrmypdf

# This script works, but it's not very efficient. It's better to process batches of files, but that's a different script and I haven't figured it out.

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

def process_pdf_with_document_ai(project_id, location, processor_id, pdf_in):
    """Extracts text from a PDF using Google Document AI."""
    # Instantiate the Document AI client
    client = documentai.DocumentProcessorServiceClient()

    # Set processor details
    name = f"projects/{project_id}/locations/{location}/processors/{processor_id}"

    # Read the PDF file
    with open(pdf_in, "rb") as file:
        pdf_content = file.read()

    # Create the request
    raw_document = documentai.RawDocument(content=pdf_content, mime_type="application/pdf")
    request = documentai.ProcessRequest(name=name, raw_document=raw_document)

    # Process the document: https://cloud.google.com/document-ai/docs/handle-response#text_layout_and_quality_scores 
    result = client.process_document(request=request)

    # Convert to hOCR format
    wrapped_document = document.Document.from_documentai_document(result.document)
    hocr_string = wrapped_document.export_hocr_str(title=os.path.basename(pdf_in))
    
    return hocr_string

def save_hocr_to_file(hocr_string, output_path):
    """Saves the hOCR string to a file."""
    with open(output_path, 'w', encoding='utf-8') as f:
        f.write(hocr_string)
    print(f"hOCR file saved to: {output_path}")

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

# Google Document AI parameters
project_id = "ai-aerospace"
location = "us"
processor_id = "baa26d1093093c7e"   # get from google cloud console

# Define the path to the folder containing the PDFs
# pdf_folder_path = "/Users/danmueller/Library/CloudStorage/GoogleDrive-dsm@danmueller.pro/My Drive/AI Aerospace/Documents/Aerospace Mechanisms/ESMATS/process_queue"
pdf_folder_path = "data/"

# List all PDF files in the folder
pdf_files = [
    f for f in os.listdir(pdf_folder_path)
    if f.endswith('.pdf') and '_reocr' not in os.path.basename(f) and '_without_text' not in os.path.basename(f)
]

# Iterate over each PDF file and process it with a progress bar
for pdf_file in tqdm(pdf_files, desc="Processing PDFs"):
    print(f"Processing: {pdf_file}")
    input_pdf_path = os.path.join(pdf_folder_path, pdf_file)
    base_name = os.path.basename(input_pdf_path)
    name, ext = os.path.splitext(base_name)
    pdf_without_text_path = os.path.join(pdf_folder_path, f"{name}_without_text{ext}")
    output_pdf_path = os.path.join(pdf_folder_path, f"{name}_reocr{ext}")

    # Step 1: Remove the existing OCR text layer
    print("Removing existing OCR text layer...")
    remove_text_by_rasterizing(input_pdf_path, pdf_without_text_path)

    # Step 2: Extract text with Google Document AI
    print("Extracting text using Google Document AI...")
    hocr_output = process_pdf_with_document_ai(project_id, location, processor_id, pdf_without_text_path)
    hocr_output_path = os.path.join(pdf_folder_path, f"{name}_ocr.hocr")
    print(f"Saving hOCR to file: {hocr_output_path}")
    save_hocr_to_file(hocr_output, hocr_output_path)

    # Convert hOCR to PDF
    print(f"Converting hOCR to PDF: {output_pdf_path}")
    convert_hocr_to_pdf(hocr_output_path, pdf_without_text_path, output_pdf_path)
