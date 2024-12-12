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

index_name='unstructured-test-openai-large'
cloud_bucket_path='gs://processing-pdfs'
directory_with_results='./results'

if __name__ == "__main__":
    Pipeline.from_configs(
        # context=ProcessorConfig(),
        context=ProcessorConfig(disable_parallelism=True,
                                work_dir=directory_with_results),
        indexer_config=GcsIndexerConfig(remote_url=cloud_bucket_path),
        downloader_config=GcsDownloaderConfig(download_dir=directory_with_results),
        source_connection_config=GcsConnectionConfig(
            access_config=GcsAccessConfig(
                service_account_key=os.getenv("GCS_SERVICE_ACCOUNT_KEY")
            )
        ),
        # Partition locally 
        # partitioner_config=PartitionerConfig(
        #     partition_by_api=False,
        #     strategy="hi_res",
        # ),
        # Partition by API
        partitioner_config=PartitionerConfig(
            partition_by_api=True,
            api_key=os.getenv("UNSTRUCTURED_API_KEY"),
            partition_endpoint=os.getenv("UNSTRUCTURED_API_URL"),
            strategy="hi_res",    # fast, cheap. hi_res better, 10x more expensive
        ),
        chunker_config=ChunkerConfig(chunking_strategy="by_title",
                                     chunk_max_characters=2048,
                                     chunk_n_workers=1500,
                                     chunk_overlap=160
        ),
        embedder_config=EmbedderConfig(
            embedding_provider="openai",
            embedding_model_name="text-embedding-3-large",
            embedding_api_key=os.getenv("OPENAI_API_KEY"),
        ),

        # Export to Pinecone
        # destination_connection_config=PineconeConnectionConfig(
        #     access_config=PineconeAccessConfig(
        #         api_key=os.getenv("PINECONE_API_KEY")
        #     ),
        #     index_name=index_name
        # ),
        # stager_config=PineconeUploadStagerConfig(),
        # uploader_config=PineconeUploaderConfig()

        # Export to local directory
        uploader_config=LocalUploaderConfig(output_dir=directory_with_results)
    ).run()