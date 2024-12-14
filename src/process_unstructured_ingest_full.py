import os
from dotenv import load_dotenv,find_dotenv
import time

from pinecone import Pinecone, ServerlessSpec

from unstructured_ingest.v2.pipeline.pipeline import Pipeline
from unstructured_ingest.v2.interfaces import ProcessorConfig

from unstructured_ingest.v2.processes.connectors.fsspec.gcs import (
    GcsIndexerConfig,
    GcsDownloaderConfig,
    GcsConnectionConfig,
    GcsAccessConfig
)

from unstructured_ingest.v2.processes.connectors.local import (
    LocalIndexerConfig,
    LocalDownloaderConfig,
    LocalConnectionConfig,
    LocalUploaderConfig
)

from unstructured_ingest.v2.processes.connectors.pinecone import (
    PineconeConnectionConfig,
    PineconeAccessConfig,
    PineconeUploaderConfig,
    PineconeUploadStagerConfig
)
from unstructured_ingest.v2.processes.partitioner import PartitionerConfig
from unstructured_ingest.v2.processes.chunker import ChunkerConfig
from unstructured_ingest.v2.processes.embedder import EmbedderConfig

load_dotenv(find_dotenv(), override=True)

directory_with_results='./results'
disable_parallelism=True
partition_local=False
partition_args={
    "coordinates": True,
    "split_pdf_page": True,
    "split_pdf_allow_failed": True
}

embed=False
pinecone_upsert=False
index_name='unstructured-test-openai-large'
cloud_bucket_path='gs://processing-pdfs'

if __name__ == "__main__":
    # Partitioning
    if partition_local: 
        partitioner_config=PartitionerConfig(
            partition_by_api=False,
            strategy="hi_res",
        )
    else:
        partitioner_config=PartitionerConfig(
            partition_by_api=True,
            api_key=os.getenv("UNSTRUCTURED_API_KEY"),
            partition_endpoint=os.getenv("UNSTRUCTURED_API_URL"),
            strategy="hi_res",    # fast, cheap. hi_res better, 10x more expensive
            additional_partition_args=partition_args
        )
    
    # Embedding
    if embed:
        embedder_config=EmbedderConfig(
            embedding_provider="openai",
            embedding_model_name="text-embedding-3-large",
            embedding_api_key=os.getenv("OPENAI_API_KEY"),
        )
    else:
        embedder_config=None

    # Export to Pinecone
    if pinecone_upsert:
        destination_connection_config=PineconeConnectionConfig(
            access_config=PineconeAccessConfig(
                api_key=os.getenv("PINECONE_API_KEY")
            ),
            index_name=index_name
        )
        uploader_config=None
        stager_config=PineconeUploadStagerConfig(),
        uploader_config=PineconeUploaderConfig()
    else:
        destination_connection_config=None
        uploader_config=LocalUploaderConfig(output_dir=directory_with_results)
        stager_config=None
        uploader_config=None
    
    # Structured output
    Pipeline.from_configs(
        context=ProcessorConfig(disable_parallelism=disable_parallelism,
                                work_dir=directory_with_results),
        indexer_config=GcsIndexerConfig(remote_url=cloud_bucket_path),
        downloader_config=GcsDownloaderConfig(download_dir=directory_with_results),
        source_connection_config=GcsConnectionConfig(
            access_config=GcsAccessConfig(
                service_account_key=os.getenv("GCS_SERVICE_ACCOUNT_KEY")
            )
        ),
        partitioner_config=partitioner_config,
        chunker_config=ChunkerConfig(chunking_strategy="by_title",
                                     chunk_max_characters=2000,
                                     chunk_n_workers=1500,
                                     chunk_overlap=160
        ),
        embedder_config=embedder_config,
        destination_connection_config=destination_connection_config,
        stager_config=stager_config,
        uploader_config=uploader_config
    ).run()

