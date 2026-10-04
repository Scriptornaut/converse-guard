"""Automated mock test suite for verify_boto3.py client behaviors."""
import os
import sys
from unittest.mock import MagicMock, patch
import pytest
from botocore.exceptions import ClientError, EventStreamError

# Ensure verify_boto3 is importable
sys.path.insert(0, os.path.dirname(__file__))
import verify_boto3


def test_missing_key_rejection():
    """Assert client rejects execution with code 2 when gateway master key is missing."""
    with patch("sys.argv", ["verify_boto3.py", "--key", ""]):
        with pytest.raises(SystemExit) as exc_info:
            verify_boto3.main()
        assert exc_info.value.code == 2


def test_incomplete_stream_detection():
    """Assert client detects missing terminal messageStop, exits 1, and closes both stream and client."""
    mock_bedrock = MagicMock()
    mock_stream = MagicMock()

    mock_stream.__iter__.return_value = iter([
        {"contentBlockDelta": {"delta": {"text": "Incomplete chunk"}}}
    ])
    mock_bedrock.converse_stream.return_value = {"stream": mock_stream}

    with patch("boto3.client", return_value=mock_bedrock), \
         patch("sys.argv", ["verify_boto3.py", "--key", "sk-test", "--prompt", "test"]):
        with pytest.raises(SystemExit) as exc_info:
            verify_boto3.main()
        assert exc_info.value.code == 1
        mock_stream.close.assert_called_once()
        mock_bedrock.close.assert_called_once()


def test_event_stream_error_handling():
    """Assert client catches EventStreamError, reports neutral stream error, exits 1, and closes resources."""
    mock_bedrock = MagicMock()
    mock_stream = MagicMock()

    def raise_stream_err():
        yield {"contentBlockDelta": {"delta": {"text": "Prefix"}}}
        raise EventStreamError(
            error_response={"Error": {"Code": "internalServerException", "Message": "Inspection or provider failure"}},
            operation_name="ConverseStream"
        )

    mock_stream.__iter__.side_effect = raise_stream_err
    mock_bedrock.converse_stream.return_value = {"stream": mock_stream}

    with patch("boto3.client", return_value=mock_bedrock), \
         patch("sys.argv", ["verify_boto3.py", "--key", "sk-test"]):
        with pytest.raises(SystemExit) as exc_info:
            verify_boto3.main()
        assert exc_info.value.code == 1
        mock_stream.close.assert_called_once()
        mock_bedrock.close.assert_called_once()


def test_client_error_handling():
    """Assert client catches ClientError (e.g. HTTP 403 pre-call block), exits 1, and closes client."""
    mock_bedrock = MagicMock()
    mock_bedrock.converse_stream.side_effect = ClientError(
        error_response={"Error": {"Code": "403", "Message": "Input prompt blocked by security policy"}},
        operation_name="ConverseStream"
    )

    with patch("boto3.client", return_value=mock_bedrock), \
         patch("sys.argv", ["verify_boto3.py", "--key", "sk-test"]):
        with pytest.raises(SystemExit) as exc_info:
            verify_boto3.main()
        assert exc_info.value.code == 1
        mock_bedrock.close.assert_called_once()


def test_successful_stream_execution():
    """Assert client iterates stream to completion, prints output, exits 0, and closes resources."""
    mock_bedrock = MagicMock()
    mock_stream = MagicMock()

    mock_stream.__iter__.return_value = iter([
        {"contentBlockDelta": {"delta": {"text": "Hello world"}}},
        {"messageStop": {"stopReason": "end_turn"}}
    ])
    mock_bedrock.converse_stream.return_value = {"stream": mock_stream}

    with patch("boto3.client", return_value=mock_bedrock), \
         patch("sys.argv", ["verify_boto3.py", "--key", "sk-test"]):
        verify_boto3.main()

    mock_stream.close.assert_called_once()
    mock_bedrock.close.assert_called_once()
