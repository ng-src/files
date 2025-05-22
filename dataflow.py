import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
from apache_beam.pvalue import TaggedOutput
import json
import os
import hashlib  # Import hashlib for SHA-256
from jsonschema import validate, ValidationError
from datetime import datetime

# --- Configuration ---
PROJECT_ID = 'your-gcp-project-id'
INPUT_PUBSUB_TOPIC = 'projects/{}/topics/your-input-pubsub-topic'.format(PROJECT_ID)
VALIDATED_PUBSUB_TOPIC = 'projects/{}/topics/your-validated-pubsub-topic'.format(
    PROJECT_ID)
DLT_PUBSUB_TOPIC = 'projects/{}/topics/your-dlt-pubsub-topic'.format(PROJECT_ID)

SCHEMA_FILE_PATH = 'event_schema.json'

# Side output tag for invalid messages
INVALID_SCHEMA_TAG = 'invalid_schema'


def generate_deterministic_message_id(message_dict):
    """
    Generates a deterministic message ID from a dictionary (parsed JSON).

    Args:
        message_dict (dict): The Python dictionary representing the JSON message.

    Returns:
        str: A hexadecimal string representing the SHA-256 hash of the
             canonical JSON representation.
    """
    if not isinstance(message_dict, dict):
        raise TypeError("Input message must be a dictionary.")

    canonical_string = json.dumps(message_dict, sort_keys=True, separators=(',', ':'))
    encoded_string = canonical_string.encode('utf-8')
    hasher = hashlib.sha256(encoded_string)
    message_id = hasher.hexdigest()

    return message_id


# --- Custom DoFn for Schema Validation ---
class ValidateJsonSchema(beam.DoFn):
    """
    A DoFn that validates JSON messages against a predefined schema.
    Emits valid messages to the main output (with deterministic message ID and attributes)
    and invalid messages to a side output (DLT).
    """

    def __init__(self, schema_path):
        self.schema_path = schema_path
        self._schema = None  # To be loaded on first element

    def setup(self):
        # Load the schema once per worker process.
        # This is more efficient than loading for every message.
        try:
            with open(self.schema_path, 'r') as f:
                self._schema = json.load(f)
            print(f"Worker {os.getpid()}: Schema loaded successfully from {self.schema_path}")
        except FileNotFoundError:
            raise ValueError(f"Schema file not found at {self.schema_path}")
        except json.JSONDecodeError:
            raise ValueError(f"Invalid JSON in schema file {self.schema_path}")

    def process(self, element):
        """
        Processes each Pub/Sub message (as a dictionary).
        `element` is a PubsubMessage object from ReadFromPubSub(with_attributes=True).
        """
        original_pubsub_message_id = element.attributes.get('message_id', 'N/A')
        original_publish_time = element.attributes.get('publish_time', 'N/A')
        original_data_bytes = element.data
        original_attributes = element.attributes  # Keep all original attributes

        try:
            # Decode Pub/Sub message data (bytes to string, then parse JSON)
            data_str = original_data_bytes.decode('utf-8')
            json_data = json.loads(data_str)

            # Perform schema validation
            validate(instance=json_data, schema=self._schema)

            # If validation succeeds:
            print(f"Message {original_pubsub_message_id} is VALID. Processing...")

            # --- Generate Deterministic Message ID ---
            # Call the deterministic ID function with the parsed JSON data
            dataflow_message_id = generate_deterministic_message_id(json_data)

            # Create a dictionary for attributes for the validated message
            validated_attributes = {
                "dataflow_message_id": dataflow_message_id,
                "original_pubsub_id": original_pubsub_message_id,
                "original_publish_time": original_publish_time,
                **original_attributes  # Merge all original attributes
            }
            # Ensure all attribute values are strings
            validated_attributes = {k: str(v) for k, v in validated_attributes.items()}

            # Yield a dictionary suitable for WriteToPubSub(with_attributes=True)
            yield {
                'data': original_data_bytes,  # The validated message payload
                'attributes': validated_attributes
            }
            beam.metrics.Metrics.counter('validation', 'valid_messages').inc()


        except json.JSONDecodeError as e:
            # Handle cases where the message data isn't valid JSON
            error_details = {
                "original_message_id": original_pubsub_message_id,
                "original_publish_time": original_publish_time,
                "original_data_base64": original_data_bytes.decode('utf-8', errors='ignore'),  # Store as string for DLT
                "validation_error": f"Invalid JSON format: {e}",
                "error_type": "JSON_DECODE_ERROR",
                "processing_timestamp": datetime.utcnow().isoformat() + 'Z'
            }
            # Yield to side output (DLT)
            yield TaggedOutput(INVALID_SCHEMA_TAG, json.dumps(error_details).encode('utf-8'))
            beam.metrics.Metrics.counter('validation', 'json_decode_errors').inc()
            print(f"Message {original_pubsub_message_id} (JSON_DECODE_ERROR) sent to DLT.")

        except ValidationError as e:
            # Handle schema validation errors
            error_details = {
                "original_message_id": original_pubsub_message_id,
                "original_publish_time": original_publish_time,
                "original_data_base64": original_data_bytes.decode('utf-8', errors='ignore'),
                "validation_error": e.message,
                "error_path": list(e.path),
                "validator": e.validator,
                "validator_value": str(e.validator_value),
                "error_type": "SCHEMA_VALIDATION_ERROR",
                "processing_timestamp": datetime.utcnow().isoformat() + 'Z'
            }
            # Yield to side output (DLT)
            yield TaggedOutput(INVALID_SCHEMA_TAG, json.dumps(error_details).encode('utf-8'))
            beam.metrics.Metrics.counter('validation', 'schema_validation_errors').inc()
            print(f"Message {original_pubsub_message_id} (SCHEMA_VALIDATION_ERROR) sent to DLT.")

        except Exception as e:
            # Catch any other unexpected errors
            error_details = {
                "original_message_id": original_pubsub_message_id,
                "original_publish_time": original_publish_time,
                "original_data_base64": original_data_bytes.decode('utf-8', errors='ignore'),
                "validation_error": f"Unexpected error: {e}",
                "error_type": "UNEXPECTED_ERROR",
                "processing_timestamp": datetime.utcnow().isoformat() + 'Z'
            }
            # Yield to side output (DLT)
            yield TaggedOutput(INVALID_SCHEMA_TAG, json.dumps(error_details).encode('utf-8'))
            beam.metrics.Metrics.counter('validation', 'unexpected_errors').inc()
            print(f"Message {original_pubsub_message_id} (UNEXPECTED_ERROR) sent to DLT.")


# --- Pipeline Definition ---
def run_pipeline():
    # Options for the pipeline
    options = PipelineOptions()
    # Required for streaming
    options.view_as(StandardOptions).streaming = True

    # For running on Dataflow, you'll need to specify these:
    # from apache_beam.options.pipeline_options import GoogleCloudOptions, WorkerOptions, SetupOptions
    # options.view_as(GoogleCloudOptions).project = PROJECT_ID
    # options.view_as(GoogleCloudOptions).region = 'us-central1' # Choose your region
    # options.view_as(GoogleCloudOptions).job_name = 'json-schema-validator'
    # options.view_as(GoogleCloudOptions).staging_location = 'gs://your-bucket-name/staging'
    # options.view_as(GoogleCloudOptions).temp_location = 'gs://your-bucket-name/temp'
    # options.view_as(WorkerOptions).num_workers = 1 # Start with 1, scale as needed
    # options.view_as(WorkerOptions).machine_type = 'n1-standard-1' # Adjust machine type
    # options.view_as(SetupOptions).save_main_session = True # For custom DoFns, often needed
    # options.view_as(SetupOptions).sdk_location = 'container' # Use container images for portability

    with beam.Pipeline(options=options) as p:
        # Read messages from Pub/Sub
        pubsub_messages = (
                p
                | "ReadFromPubSub" >> beam.io.ReadFromPubSub(topic=INPUT_PUBSUB_TOPIC, with_attributes=True)
            # The 'with_attributes=True' makes each element a PubsubMessage object,
            # allowing access to data and attributes like message_id.
        )

        # Apply the validation DoFn with side outputs
        validation_results = (
                pubsub_messages
                | "ValidateJson" >> beam.ParDo(
            ValidateJsonSchema(SCHEMA_FILE_PATH)
        ).with_outputs(INVALID_SCHEMA_TAG, main='valid_messages')
        )

        # Separate the valid and invalid messages
        valid_messages_with_attributes = validation_results.valid_messages
        invalid_messages_for_dlt = validation_results[INVALID_SCHEMA_TAG]

        # Write valid messages to the validated topic
        # The elements here are dictionaries {'data': bytes, 'attributes': dict}
        (
                valid_messages_with_attributes
                | "WriteValidToPubSub" >> beam.io.WriteToPubSub(topic=VALIDATED_PUBSUB_TOPIC, with_attributes=True)
        )

        # Write invalid messages (with error details) to the DLT topic
        # The elements here are bytes (JSON string of error_details)
        (
                invalid_messages_for_dlt
                | "WriteInvalidToDLT" >> beam.io.WriteToPubSub(topic=DLT_PUBSUB_TOPIC)
        )

        # You can add more sinks here, e.g., writing to BigQuery:
        # (
        #     valid_messages_with_attributes
        #     | 'ParseValidJsonToDict' >> beam.Map(lambda x: json.loads(x['data'].decode('utf-8')))
        #     | 'WriteValidToBigQuery' >> beam.io.WriteToBigQuery(
        #         'your_project_id:your_dataset.your_valid_table',
        #         schema='SCHEMA_AUTODETECT', # Or define a BigQuery schema
        #         write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
        #         create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED
        #     )
        # )

        # (
        #     invalid_messages_for_dlt
        #     | 'ParseInvalidJsonToDict' >> beam.Map(json.loads)
        #     | 'WriteInvalidToBigQuery' >> beam.io.WriteToBigQuery(
        #         'your_project_id:your_dataset.your_dlt_table',
        #         schema='SCHEMA_AUTODETECT', # Or define a BigQuery schema
        #         write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
        #         create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED
        #     )
        # )


if __name__ == '__main__':
    # Create dummy schema file for local testing
    with open(SCHEMA_FILE_PATH, 'w') as f:
        f.write(json.dumps({
            "$schema": "http://json-schema.org/draft-07/schema#",
            "title": "SensorEvent",
            "description": "Schema for a sensor data event",
            "type": "object",
            "required": ["deviceId", "timestamp", "temperature", "humidity"],
            "properties": {
                "deviceId": {"type": "string"},
                "timestamp": {"type": "string", "format": "date-time"},
                "temperature": {"type": "number", "minimum": -50, "maximum": 100},
                "humidity": {"type": "number", "minimum": 0, "maximum": 100}
            },
            "additionalProperties": false
        }, indent=2))

    print("\n--- IMPORTANT: Please configure your GCP project, topics, and staging/temp locations ---")
    print(f"Input Topic: {INPUT_PUBSUB_TOPIC}")
    print(f"Validated Topic: {VALIDATED_PUBSUB_TOPIC}")
    print(f"DLT Topic: {DLT_PUBSUB_TOPIC}")
    print(
        "\nTo run on Dataflow, uncomment and configure `PipelineOptions` for `GoogleCloudOptions` and `WorkerOptions`.")
    print("Starting pipeline...")
    run_pipeline()