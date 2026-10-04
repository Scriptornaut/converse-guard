"""Native Converse pre-call hook; OSS CustomLogger registration."""
import uuid
from fastapi import HTTPException
from litellm.integrations.custom_logger import CustomLogger
from native_guard import get_scanner, user_text, InspectionFailure

class NativeInput(CustomLogger):
    async def async_pre_call_hook(self, user_api_key_dict, cache, data, call_type):
        if call_type != 'allm_passthrough_route' or data.get('custom_llm_provider') != 'bedrock':
            return data
        endpoint = data.get('endpoint', '')
        if not endpoint.endswith('/converse-stream'):
            raise HTTPException(400, 'This POC only supports native ConverseStream')
        try:
            text = user_text(data.get('data', {}))
            # Transaction IDs are separate for IN and OUT in this POC.
            await get_scanner().scan(text, 'IN', str(uuid.uuid4()))
        except InspectionFailure as exc:
            raise HTTPException(exc.status, exc.message) from exc
        return data  # Original system, messages and tools remain untouched.

proxy_handler_instance = NativeInput()
