from google.cloud import bigquery
from google.cloud.exceptions import NotFound
import logging


def get_table_schema(dataset, table):
    """Get BigQuery Table Schema."""
    logging.info("getting table schema")
    bigquery_client = bigquery.Client()
    dataset_ref = bigquery_client.dataset(dataset)
    bg_tableref = bigquery.table.TableReference(dataset_ref, table)
    bg_table = bigquery_client.get_table(bg_tableref)
    return bg_table.schema


class BigqueryHelper:
    def __init__(
        self,
        project_id,
        pubsub_topic,
        dataset,
        table,
        json_data,
    ):
        self.project_id = project_id
        self.pubsub_topic = pubsub_topic
        self.dataset = dataset
        self.table = table
        self.json_data = json_data

    def load_table_from_json(self):
        """Loads data formatted in JSON into their respective tables. Creates table
        if it doesn't exist (or simply appends) by automatically detecting the schema.
        """

        client = bigquery.Client()

        # generates table id based on project, dataset and table.
        table_id = ".".join((self.project_id, self.dataset, self.table))

        job_config = bigquery.LoadJobConfig(
            autodetect=True,
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
            ignore_unknown_values=True,
            write_disposition="WRITE_APPEND",
            schema_update_options=[bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION],
        )
        try:
            load_job = client.load_table_from_json(
                self.json_data, table_id, num_retries=10, job_config=job_config
            )
            load_job.result()  # Waits for the job to complete.
            destination_table = client.get_table(table_id)

            logging.info("Loaded {} rows.".format(destination_table.num_rows))

        except Exception as exception:
            logging.error(exception)
            raise
        finally:
            logging.info("Finally inserted data.")
