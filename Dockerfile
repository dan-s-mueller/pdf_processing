# Use an official Python runtime as a parent image
FROM python:3.11.5-bookworm

# Set the working directory in the container
WORKDIR /src

# Install system dependencies
RUN apt-get update && apt-get install -y \
    ocrmypdf

# Copy the current directory contents into the container at /app
COPY . /src

# Install any needed packages
RUN pip install --upgrade pip
RUN pip install google-cloud-storage

# Make port 8080 available to the world outside this container
EXPOSE 8080

# Define environment variable
ENV PORT=8080

# Run app.py when the container launches
CMD ["python", "process_pdfs.py"]