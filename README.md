# converse-guard

An independent reference implementation for integrating **Zscaler AI Guard** runtime security inspection with **LiteLLM OSS (v1.103.0)** on native **Amazon Bedrock `converse_stream`** routes.

---

## What It Does

`converse-guard` provides the custom ASGI middleware and pre-call hooks necessary to inspect native Amazon Bedrock conversational streams passing through a self-hosted LiteLLM gateway (`POST /bedrock/model/{model}/converse-stream`).

On the tested LiteLLM OSS 1.103.0 native ConverseStream path, standard configurations do not inspect binary AWS EventStream frames. This implementation bridges that gap by coupling an inbound pre-call hook with an outbound streaming middleware, enabling applications calling Bedrock via the AWS SDK (Boto3) to stream directly through LiteLLM with real-time prompt injection prevention (inbound) and data loss prevention (outbound) enforced by Zscaler AI Guard.

---

## Who It Is For

This project is intended for cloud security engineers, AI platform teams, and solutions architects who:
- Host an internal AI gateway using LiteLLM OSS.
- Consume Amazon Bedrock models through native AWS SDK client calls (`boto3.client('bedrock-runtime').converse_stream`) rather than OpenAI-compatible abstractions (`/v1/chat/completions`).
- Require centralized, bidirectional policy enforcement via Zscaler AI Guard in Detection-as-a-Service (DAS) mode.

---

## How Requests and Streamed Responses Are Inspected

### 1. Inbound Request Inspection (`NativeInput`)
- **Target Selection:** When an application calls `converse_stream`, the pre-call hook intercepts the payload and scans **only the text from the latest user message containing text**. Tool-result-only user turns are skipped, allowing an earlier text-bearing user message to be evaluated.
- **Request Integrity:** System instructions, tool configurations, tool results, and previous assistant turns are excluded from the inbound AI Guard scan. However, the **complete, unmodified request payload is preserved and passed to Amazon Bedrock**.
- **Verdict Handling:** If AI Guard returns a `BLOCK` verdict, the gateway rejects the request with an HTTP 403 error before any call reaches Amazon Bedrock. If the inspection service is unavailable or throttled, the gateway fails closed with an HTTP 503 error.

### 2. Outbound Response Inspection (`ConverseOutputMiddleware` & `StreamGate`)
- **EventStream Decoding:** Amazon Bedrock streams responses as binary AWS EventStream frames (`application/vnd.amazon.eventstream`). The middleware validates frame headers and 32-bit CRCs in real time, extracting event payloads (`contentBlockDelta`, `messageStop`).
- **Cumulative Batch Scanning:** Generated text tokens are held in an in-memory buffer. When the number of new characters since the previous scan reaches the batch threshold (`AIGUARD_BATCH_CHARS`, default: 256) or the stream finishes (`messageStop`), the middleware submits the **cumulative generated text prefix** to Zscaler AI Guard with direction `OUT`.
- **Progressive Frame Release:** If AI Guard approves the text (`ALLOW` or `DETECT`), all binary frames comprising that text segment are immediately released downstream to the client.
- **Tool Call Handling:** Once a tool-use block is encountered, pending output is held until `messageStop`, allowing accumulated tool arguments to be inspected together before release.
- **Irrevocable Release Boundary:** In batch streaming mode, text frames that have already been approved and delivered across the network **cannot be recalled or redacted if a subsequent chunk triggers a block**. If a block occurs mid-stream, the gateway terminates the connection immediately by emitting a native AWS EventStream `internalServerException` frame to prevent further transmission.

---

## What It Does Not Cover

- **Non-Text Modalities:** This implementation handles text and tool-call arguments only. Images, audio, document attachments, citations, and model reasoning/thinking blocks are unsupported. Unsupported input can produce an HTTP 400 error; unsupported output produces an HTTP 503 error before release, or a native stream error after streaming has begun.
- **Tool Execution:** The gateway scans tool-call JSON arguments as text before releasing them to the client; it does not validate external API permissions or execute tools.
- **Provenance Within User Text:** The adapter assumes text within `role="user"` represents external user input. If an application combines untrusted user text with internal instructions or templates into the same user message block, AI Guard evaluates the entire combined string.
- **Full Buffering Trade-Off:** Full mode (`AIGUARD_STREAM_MODE=full`) withholds generated content until the final inspection permits release. It prevents early release but does not guarantee that detectors identify every policy violation.

---

## Validation Scope & Provenance

- **Tested Baseline:** Developed and tested against **LiteLLM OSS v1.103.0** on Python 3.12 with Botocore 1.43.106 and Uvicorn 0.54.0.
- **Validation Distinction:**
  - *Live Lab Deployment:* The adapter mechanics and live streaming integration were validated against live Amazon Bedrock (`us.anthropic.claude-haiku-4-5-20251001-v1:0` in `us-east-1`) and production Zscaler AI Guard endpoints in an active AWS demo environment.
  - *Standalone Draft Verification:* The files in this standalone repository draft have been validated locally using mocked Bedrock EventStreams, simulated DAS responses across an automated 24-test suite (`test_native_guard.py`), and local Docker container startup health checks. Live cloud services were not contacted during local package verification.

---

## Quick Start

### 1. Prerequisites
- Docker and Docker Compose (or standalone Docker Engine).
- Active Zscaler AI Guard tenant with an API Bearer token and an integer Detection Policy ID.
- AWS account with access to your target Amazon Bedrock model or inference profile, and IAM credentials permitting `bedrock:InvokeModelWithResponseStream`.
- Client SDK: Python 3.12 with pinned Boto3 and Botocore:
  ```bash
  pip install boto3==1.43.106 botocore==1.43.106
  ```

### 2. Environment Configuration
Copy `.env.example` to create your local `.env` configuration file:
```bash
cp .env.example .env
```
Edit `.env` and configure:
- `LITELLM_MASTER_KEY`: Secret master key securing the gateway (e.g., `sk-my-secure-master-key`). **Clients must supply this exact key to authenticate.**
- `BEDROCK_MODEL_ID`: Upstream Bedrock model or inference profile (e.g., `bedrock/us.anthropic.claude-haiku-4-5-20251001-v1:0`).
- `AWS_REGION_NAME`: AWS region for Bedrock API requests (e.g., `us-east-1`).
- `AIGUARD_URL`: Full detection endpoint URL for your tenant cloud (e.g., `https://api.us1.zseclipse.net/v1/detection/execute-policy`).
- `AIGUARD_API_KEY`: Your Zscaler AI Guard API Bearer token.
- `AIGUARD_POLICY_ID`: Your active integer Detection Policy ID (e.g., `1256`).

### 3. Build & Start the Gateway

#### Option A: Docker Compose (Recommended)
Builds the local image (`converse-guard:local`) and starts the container:
```bash
docker compose up --build -d
```

#### Option B: Standalone Docker
```bash
docker build -t converse-guard:local .
docker run -d \
  --name converse-guard \
  -p 127.0.0.1:4000:4000 \
  --env-file .env \
  converse-guard:local
```

> **Network Binding Note:** The default host port mapping `127.0.0.1:4000:4000` binds the gateway strictly to the local loopback interface, assuming client callers run on the same host. If calling the gateway from another host or container network, adjust the port mapping or bind to your private gateway interface.

### 4. Verify Gateway Liveliness
Test that the gateway container is running. Replace `<YOUR_CONFIGURED_LITELLM_MASTER_KEY>` with the exact key set in your `.env`:
```bash
curl -si -H "Authorization: Bearer <YOUR_CONFIGURED_LITELLM_MASTER_KEY>" \
  http://127.0.0.1:4000/health/liveliness
```
Expected response:
```http
HTTP/1.1 200 OK
content-type: application/json

"I'm alive!"
```

### 5. Verify Native ConverseStream via Boto3 Client
Run the included verification client using the same configured master key (quoted to prevent shell redirection):
```bash
python3 verify_boto3.py \
  --endpoint http://127.0.0.1:4000/bedrock \
  --model bedrock-model \
  --key "<YOUR_CONFIGURED_LITELLM_MASTER_KEY>" \
  --prompt "Describe a scenic mountain getaway in 20 words."
```

#### Client Connection & Credential Separation:
- **Client vs. Gateway Credentials:** The verification client authenticates to the LiteLLM gateway using `--key` (transmitted as an HTTP Bearer token via `AWS_BEARER_TOKEN_BEDROCK`). The client does **not** require direct AWS Bedrock IAM credentials. In turn, the LiteLLM gateway authenticates upstream to Amazon Bedrock using its own host environment credentials (e.g., an IAM instance profile or AWS environment variables).
- **Endpoint URL (`--endpoint`):** Must point to the gateway with the `/bedrock` path prefix (`http://127.0.0.1:4000/bedrock`). Boto3 automatically appends `/model/{modelId}/converse-stream`, routing cleanly to LiteLLM's native passthrough handler.
- **Model Routing (`--model`):** The client requests the model alias `bedrock-model`. LiteLLM resolves this alias against `config.yaml` and routes the request upstream to the Bedrock model configured in `BEDROCK_MODEL_ID`.

---

## License & Attribution

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License in the `LICENSE` file or at:

[http://www.apache.org/licenses/LICENSE-2.0](http://www.apache.org/licenses/LICENSE-2.0)

> **converse-guard is a personal reference project for integrating LiteLLM OSS, Amazon Bedrock, and Zscaler AI Guard. It is not an official vendor product, and no vendor endorsement is implied. Product names identify compatibility.**
