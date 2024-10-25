# Use an official Python runtime as a parent image
FROM python:3.11.5-bookworm

# Set the working directory in the container
WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    ocrmypdf \
    build-essential \
    autoconf \
    automake \
    libtool \
    libleptonica-dev \
    git \
    && rm -rf /var/lib/apt/lists/*

# Build and install jbig2enc separately
RUN git clone https://github.com/agl/jbig2enc.git \
    && cd jbig2enc \
    && ./autogen.sh \
    && ./configure \
    && make \
    && make install \
    && cd .. \
    && rm -rf jbig2enc

# Copy the current directory contents into the container at /app
COPY /src /app
# COPY ai-aerospace.json /app/ai-aerospace.json # Required for local testing

# Install any needed packagess
RUN pip install --upgrade pip
RUN pip install google-cloud-storage
RUN pip install ocrmypdf

# Make port 8080 available to the world outside this container
EXPOSE 8080

# Define environment variable
ENV PORT=8080
# ENV GOOGLE_APPLICATION_CREDENTIALS=/app/ai-aerospace.json # Required for local testing

# Run app.py when the container launches
ENTRYPOINT ["python", "process_pdfs.py"]