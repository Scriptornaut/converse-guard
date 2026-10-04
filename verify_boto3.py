"""Minimal native Boto3 ConverseStream client to verify the running converse-guard gateway."""
import argparse
import os
import sys
import boto3
from botocore.config import Config
from botocore.exceptions import ClientError, EventStreamError

def main():
    parser = argparse.ArgumentParser(description="Test LiteLLM Native ConverseStream Gateway")
    parser.add_argument("--endpoint", default=os.getenv("GATEWAY_ENDPOINT", "http://127.0.0.1:4000/bedrock"),
                        help="Gateway endpoint URL (must include /bedrock prefix)")
    parser.add_argument("--model", default=os.getenv("BEDROCK_MODEL_ALIAS", "bedrock-model"),
                        help="Model alias configured in gateway config.yaml (default: bedrock-model)")
    parser.add_argument("--key", default=os.getenv("LITELLM_MASTER_KEY", ""),
                        help="Gateway master key for authentication (required)")
    parser.add_argument("--prompt", default="Describe a scenic mountain getaway in 20 words.",
                        help="User prompt text to submit")
    args = parser.parse_args()

    # Reject missing gateway key for automation safety
    master_key = args.key.strip() if args.key else ""
    if not master_key:
        print("ERROR: Gateway master key is required. Supply via --key or LITELLM_MASTER_KEY environment variable.", file=sys.stderr)
        sys.exit(2)

    # Set bearer token environment variable for Boto3 gateway authentication
    os.environ["AWS_BEARER_TOKEN_BEDROCK"] = master_key

    client = None
    stream = None
    terminal_received = False
    exit_code = 0

    try:
        # Client authenticates to gateway using Bearer token; dummy AWS keys satisfy local SDK requirements
        client = boto3.client(
            "bedrock-runtime",
            region_name=os.getenv("AWS_REGION_NAME", "us-east-1"),
            endpoint_url=args.endpoint,
            aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID", "dummy"),
            aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY", "dummy"),
            config=Config(connect_timeout=5, read_timeout=60, retries={"max_attempts": 1})
        )

        print(f"Connecting to Gateway: {args.endpoint}")
        print(f"Model Alias: {args.model}")
        print(f"Prompt: {args.prompt}\n--- Response Stream ---")

        response = client.converse_stream(
            modelId=args.model,
            system=[{"text": "You are a helpful assistant. Keep answers brief."}],
            messages=[{"role": "user", "content": [{"text": args.prompt}]}]
        )
        stream = response.get("stream")

        if stream is None:
            print("\n[ERROR]: No stream object returned in gateway response.", file=sys.stderr)
            exit_code = 1
        else:
            for event in stream:
                if "contentBlockDelta" in event:
                    delta_text = event["contentBlockDelta"].get("delta", {}).get("text", "")
                    print(delta_text, end="", flush=True)
                elif "messageStop" in event:
                    terminal_received = True
                    stop_reason = event["messageStop"].get("stopReason", "end_turn")
                    print(f"\n--- Stream Complete (StopReason: {stop_reason}) ---")

            if not terminal_received and exit_code == 0:
                print("\n[ERROR]: Stream closed unexpectedly before receiving a terminal messageStop event.", file=sys.stderr)
                exit_code = 1

    except EventStreamError as exc:
        print(f"\n[STREAM ERROR]: {exc}", file=sys.stderr)
        exit_code = 1
    except ClientError as exc:
        print(f"\n[CLIENT / GATEWAY HTTP ERROR]: {exc}", file=sys.stderr)
        exit_code = 1
    except Exception as exc:
        print(f"\n[UNEXPECTED ERROR]: {exc}", file=sys.stderr)
        exit_code = 1
    finally:
        # Explicitly close both stream and client
        if stream is not None:
            try:
                stream.close()
            except Exception:
                pass
        if client is not None:
            try:
                client.close()
            except Exception:
                pass

    if exit_code != 0:
        sys.exit(exit_code)

if __name__ == "__main__":
    main()
