# Use an official Python runtime as a parent image
FROM python:3.11.5-bookworm

# Set the working directory in the container
WORKDIR /app

# Install system dependencies including Tesseract and Ghostscript
RUN apt-get update && apt-get install -y \
    tesseract-ocr \
    libtesseract-dev \
    ghostscript \
    && rm -rf /var/lib/apt/lists/*

# Copy the current directory contents into the container at /app
COPY /src /app
COPY ai-aerospace.json /app/ai-aerospace.json

# Install any needed packages
RUN pip install --upgrade pip
RUN pip install google-cloud-storage
RUN pip install ocrmypdf

# Make port 8080 available to the world outside this container
EXPOSE 8080

# Define environment variable
ENV PORT=8080
ENV GOOGLE_APPLICATION_CREDENTIALS=/app/ai-aerospace.json

# Run app.py when the container launches
ENTRYPOINT ["python", "process_pdfs.py"]