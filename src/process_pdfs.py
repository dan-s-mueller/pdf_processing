import os
# from tqdm.notebook import tqdm

# Re-OCR AMS docs
directory=os.path.join('..','data','AMS')
documents = ['AMS_1980.pdf', 
             'AMS_1981.pdf',
             'AMS_1982.pdf',
             'AMS_1983.pdf',
             'AMS_1984.pdf',
             'AMS_1985.pdf',
             'AMS_1986.pdf',
             'AMS_1987.pdf',
             'AMS_1988.pdf',
             'AMS_1990.pdf',
             'AMS_1991.pdf',
             'AMS_1992.pdf',
             'AMS_1993.pdf',
             'AMS_1994.pdf',
             'AMS_1995.pdf',
             'AMS_1996.pdf',
             'AMS_1997.pdf',
             'AMS_1998.pdf',
             'AMS_1999.pdf']

# Re-OCR ESMAT docs from 1999-2003, which are probably pretty outdated OCRs.
directory=os.path.join('..','data','AMS','reocr')
documents = [file for file in os.listdir(directory) if file.endswith('.pdf') and any(year in file for year in ['1980', '1981', '1982'])]

for doc in tqdm(documents,desc='Document Processing'):
    print(f"Processing {doc}")
    try:
        for i in tqdm(range(2), desc=f"Processing {doc}", leave=False):
            if i == 0:
                os.system(f'ocrmypdf --tesseract-timeout 0 --continue-on-soft-render-error --force-ocr {directory}/{doc} {directory}/{doc}_stripped.pdf')   # Stripped pdf
            # elif i == 1:    
            #     os.system(f'ocrmypdf --sidecar {directory}/{doc}_strip_reocr.txt --continue-on-soft-render-error {directory}/{doc}_stripped.pdf {directory}/{doc}_strip_reocr.pdf') # Apply OCR, output file
            elif i == 1:
                os.system(f'ocrmypdf --sidecar {directory}/{doc}_reocr.txt --continue-on-soft-render-error --redo-ocr {directory}/{doc} {directory}/{doc}_reocr.pdf') # Apply OCR, output file
    except:
        print(f'Error processing {doc}')
        pass