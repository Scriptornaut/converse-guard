"""POC: native Bedrock Converse inspection around LiteLLM OSS. Text/tool-use only."""
import json
import os
import struct
import uuid
import zlib
from dataclasses import dataclass

import httpx
from botocore.eventstream import EventStreamBuffer


class InspectionFailure(Exception):
    def __init__(self, status, message):
        self.status, self.message = status, message
        super().__init__(message)


@dataclass
class Settings:
    batch_chars: int = 256
    mode: str = 'batch'
    max_content_bytes: int = 65536
    max_frame_bytes: int = 1048576
    max_pending_bytes: int = 2097152

    @classmethod
    def from_env(cls):
        value = cls(
            batch_chars=int(os.getenv('AIGUARD_BATCH_CHARS', '256')),
            mode=os.getenv('AIGUARD_STREAM_MODE', 'batch'),
            max_content_bytes=int(os.getenv('AIGUARD_MAX_CONTENT_BYTES', '65536')),
            max_frame_bytes=int(os.getenv('AIGUARD_MAX_FRAME_BYTES', '1048576')),
            max_pending_bytes=int(os.getenv('AIGUARD_MAX_PENDING_BYTES', '2097152')),
        )
        if value.mode not in {'batch', 'full'} or min(value.batch_chars, value.max_content_bytes, value.max_frame_bytes, value.max_pending_bytes) <= 0:
            raise ValueError('Invalid gateway inspection settings')
        return value


class DASScanner:
    def __init__(self):
        self.url = os.environ['AIGUARD_URL']
        self.key = os.environ['AIGUARD_API_KEY']
        self.policy = int(os.environ['AIGUARD_POLICY_ID'])
        self.settings = Settings.from_env()
        self.client = httpx.AsyncClient(timeout=float(os.getenv('AIGUARD_TIMEOUT_SECONDS', '30')))

    async def scan(self, content, direction, transaction_id):
        if not content:
            return
        if len(content.encode('utf-8')) > self.settings.max_content_bytes:
            raise InspectionFailure(503, 'Inspection content limit exceeded')
        try:
            response = await self.client.post(self.url, headers={'Authorization': 'Bearer ' + self.key}, json={
                'content': content, 'direction': direction, 'policyId': self.policy,
                'transactionId': transaction_id,
            })
            if response.status_code != 200:
                raise InspectionFailure(503, 'AI Guard inspection unavailable')
            verdict = response.json()
        except (httpx.HTTPError, ValueError) as exc:
            raise InspectionFailure(503, 'AI Guard inspection unavailable') from exc
        if not isinstance(verdict, dict) or verdict.get('throttlingDetails'):
            raise InspectionFailure(503, 'AI Guard returned an invalid or throttled result')
        action = verdict.get('action')
        if action == 'BLOCK':
            raise InspectionFailure(403, 'Blocked by AI Guard')
        if action not in {'ALLOW', 'DETECT'}:
            raise InspectionFailure(503, 'AI Guard returned no supported verdict')

    async def close(self):
        await self.client.aclose()


_scanner = None

def get_scanner():
    global _scanner
    if _scanner is None:
        _scanner = DASScanner()
    return _scanner


def user_text(body):
    """Last user message containing text, skipping tool-result-only continuations.

    This identifies user-role text, not provenance within an app-composed template.
    """
    for message in reversed(body.get('messages', [])):
        if message.get('role') != 'user':
            continue
        blocks = message.get('content', [])
        if not isinstance(blocks, list):
            raise InspectionFailure(400, 'Expected native Converse content blocks')
        parts = []
        for block in blocks:
            if not isinstance(block, dict) or len(block) != 1:
                raise InspectionFailure(400, 'Unsupported input content block')
            if 'text' in block:
                if not isinstance(block['text'], str):
                    raise InspectionFailure(400, 'Unsupported input text')
                parts.append(block['text'])
            elif 'toolResult' not in block and 'cachePoint' not in block:
                raise InspectionFailure(400, 'This POC supports text input and tool results only')
        if parts:
            text = '\n'.join(parts)
            if not text.strip():
                raise InspectionFailure(400, 'Empty user input')
            return text
    raise InspectionFailure(400, 'No user text available for inspection')


def encode_event(event, payload, exception=False):
    """AWS EventStream framing; original allowed frames are never re-encoded."""
    def header(key, value):
        key, value = key.encode(), value.encode()
        return bytes([len(key)]) + key + b'\x07' + struct.pack('!H', len(value)) + value
    pairs = [(':message-type', 'exception' if exception else 'event'),
             (':exception-type' if exception else ':event-type', event),
             (':content-type', 'application/json')]
    headers = b''.join(header(k, v) for k, v in pairs)
    body = json.dumps(payload, separators=(',', ':')).encode()
    prelude = struct.pack('!II', 16 + len(headers) + len(body), len(headers))
    raw = prelude + struct.pack('!I', zlib.crc32(prelude) & 0xffffffff) + headers + body
    return raw + struct.pack('!I', zlib.crc32(raw) & 0xffffffff)


class FrameDecoder:
    def __init__(self, maximum):
        self.buffer = bytearray()
        self.maximum = maximum

    def feed(self, data):
        self.buffer.extend(data)
        while len(self.buffer) >= 12:
            total, headers_len = struct.unpack('!II', self.buffer[:8])
            if total < 16 or total > self.maximum or headers_len > total - 16:
                raise InspectionFailure(503, 'Invalid Bedrock frame length')
            if len(self.buffer) < total:
                break
            original = bytes(self.buffer[:total])
            del self.buffer[:total]
            parser = EventStreamBuffer()
            try:
                parser.add_data(original)
                event = next(parser)
                payload = json.loads(event.payload)
            except Exception as exc:
                raise InspectionFailure(503, 'Invalid Bedrock event frame') from exc
            if not isinstance(payload, dict):
                raise InspectionFailure(503, 'Invalid Bedrock event payload')
            yield original, event.headers, payload


class StreamGate:
    def __init__(self, scanner, settings, transaction_id):
        self.scanner, self.settings, self.transaction_id = scanner, settings, transaction_id
        self.decoder = FrameDecoder(settings.max_frame_bytes)
        self.pending, self.pending_bytes = [], 0
        self.text, self.tools, self.active_blocks = '', {}, set()
        self.last_scanned_chars = 0
        self.started, self.stopped = False, False

    def content(self):
        return self.text + ('\n' + json.dumps(self.tools, ensure_ascii=False, sort_keys=True) if self.tools else '')

    async def flush(self):
        await self.scanner.scan(self.content(), 'OUT', self.transaction_id)
        approved, self.pending = self.pending, []
        self.pending_bytes = 0
        self.last_scanned_chars = len(self.text)
        return approved

    async def feed(self, data):
        for original, headers, body in self.decoder.feed(data):
            if headers.get(':message-type') != 'event':
                raise InspectionFailure(503, 'Bedrock upstream stream error')
            name = headers.get(':event-type')
            if name not in {'messageStart', 'contentBlockStart', 'contentBlockDelta', 'contentBlockStop', 'messageStop', 'metadata'}:
                raise InspectionFailure(503, 'Unsupported Bedrock event')
            if not self.started and name != 'messageStart':
                raise InspectionFailure(503, 'Missing Bedrock messageStart')
            if self.stopped:
                if name != 'metadata':
                    raise InspectionFailure(503, 'Content after Bedrock messageStop')
                yield original
                continue
            self.pending.append(original)
            self.pending_bytes += len(original)
            if self.pending_bytes > self.settings.max_pending_bytes:
                raise InspectionFailure(503, 'Pending response limit exceeded')
            if name == 'messageStart':
                if self.started or body.get('role') != 'assistant':
                    raise InspectionFailure(503, 'Invalid Bedrock messageStart')
                self.started = True
            elif name == 'contentBlockStart':
                index = body.get('contentBlockIndex')
                start = body.get('start', {})
                if not isinstance(index, int) or index in self.active_blocks or not isinstance(start, dict):
                    raise InspectionFailure(503, 'Invalid content block start')
                self.active_blocks.add(index)
                if start:
                    tool = start.get('toolUse')
                    if set(start) != {'toolUse'} or not isinstance(tool, dict) or not isinstance(tool.get('name'), str):
                        raise InspectionFailure(503, 'Unsupported output content block')
                    self.tools[index] = {'name': tool['name'], 'arguments': ''}
            elif name == 'contentBlockDelta':
                index, delta = body.get('contentBlockIndex'), body.get('delta')
                if not isinstance(index, int) or not isinstance(delta, dict) or len(delta) != 1:
                    raise InspectionFailure(503, 'Unsupported Bedrock delta')
                self.active_blocks.add(index)  # Text blocks may omit contentBlockStart.
                if 'text' in delta and isinstance(delta['text'], str) and index not in self.tools:
                    self.text += delta['text']
                elif 'toolUse' in delta and index in self.tools and isinstance(delta['toolUse'], dict) and isinstance(delta['toolUse'].get('input'), str):
                    self.tools[index]['arguments'] += delta['toolUse']['input']
                else:
                    raise InspectionFailure(503, 'Reasoning and non-text output are unsupported')
            elif name == 'contentBlockStop':
                index = body.get('contentBlockIndex')
                if index not in self.active_blocks:
                    raise InspectionFailure(503, 'Unexpected contentBlockStop')
                self.active_blocks.remove(index)
            elif name == 'messageStop':
                if self.active_blocks or body.get('stopReason') not in {'end_turn', 'tool_use', 'max_tokens', 'stop_sequence', 'guardrail_intervened', 'content_filtered'}:
                    raise InspectionFailure(503, 'Invalid Bedrock messageStop')
                self.stopped = True
            elif name == 'metadata':
                raise InspectionFailure(503, 'Metadata before messageStop')
            if len(self.content().encode('utf-8')) > self.settings.max_content_bytes:
                raise InspectionFailure(503, 'Inspection content limit exceeded')
            if self.stopped or (self.settings.mode == 'batch' and not self.tools and len(self.text) - self.last_scanned_chars >= self.settings.batch_chars):
                for item in await self.flush():
                    yield item

    def finish(self):
        if self.decoder.buffer or not self.stopped or self.pending:
            raise InspectionFailure(503, 'Incomplete Bedrock response stream')


class StreamAbort(Exception):
    pass


class ConverseOutputMiddleware:
    """Wrap actual ASGI response bytes rather than LiteLLM's unused native iterator hook."""
    def __init__(self, app, scanner=None, settings=None):
        self.app = app
        self.scanner = scanner or get_scanner()
        self.settings = settings or self.scanner.settings

    async def __call__(self, scope, receive, send):
        path = scope.get('path', '')
        if scope['type'] != 'http' or scope.get('method') != 'POST' or not path.startswith('/bedrock/model/') or not path.endswith('/converse-stream'):
            await self.app(scope, receive, send)
            return
        gate = StreamGate(self.scanner, self.settings, scope.setdefault('state', {}).get('aiguard_transaction_id', str(uuid.uuid4())))
        start, released, failed = None, False, False
        bypass = False

        async def fail(error):
            nonlocal released, failed
            if failed:
                return
            failed = True
            if released:
                # Native AWS exception; do not pretend the response ended successfully.
                payload = encode_event('internalServerException', {'message': error.message}, exception=True)
                await send({'type': 'http.response.body', 'body': payload, 'more_body': False})
            else:
                payload = json.dumps({'message': error.message}).encode()
                await send({'type': 'http.response.start', 'status': error.status, 'headers': [(b'content-type', b'application/json'), (b'content-length', str(len(payload)).encode())]})
                await send({'type': 'http.response.body', 'body': payload, 'more_body': False})

        async def guarded_send(message):
            nonlocal start, released, bypass
            if message['type'] == 'http.response.start':
                start = message
                bypass = message['status'] != 200
                if bypass:
                    await send(message)
                else:
                    ct = dict(message.get('headers', [])).get(b'content-type', b'').lower()
                    if b'application/vnd.amazon.eventstream' not in ct:
                        raise InspectionFailure(503, 'Expected a native Bedrock event stream')
                return
            if bypass or message['type'] != 'http.response.body':
                await send(message)
                return
            try:
                async for original in gate.feed(message.get('body', b'')):
                    if not released:
                        clean = dict(start)
                        clean['headers'] = [(k,v) for k,v in start.get('headers', []) if k.lower() != b'content-length']
                        await send(clean)
                        released = True
                    await send({'type':'http.response.body','body':original,'more_body':True})
                if not message.get('more_body', False):
                    gate.finish()
                    if not released:
                        raise InspectionFailure(503, 'Empty Bedrock event stream')
                    await send({'type':'http.response.body','body':b'','more_body':False})
            except InspectionFailure as exc:
                await fail(exc)
                raise StreamAbort() from exc
        try:
            await self.app(scope, receive, guarded_send)
        except Exception as exc:
            if failed:
                return  # StreamingResponse may wrap StreamAbort in an ExceptionGroup.
            if bypass:
                raise
            await fail(exc if isinstance(exc, InspectionFailure) else InspectionFailure(503, 'Gateway stream processing failed'))
