import os
os.environ['LITELLM_LOCAL_MODEL_COST_MAP']='True'
os.environ.pop('LITELLM_LICENSE',None)
import asyncio, json, struct, threading, time, socket, pathlib, importlib.metadata
import httpx, pytest
from botocore.eventstream import EventStreamBuffer
from native_guard import DASScanner, Settings, StreamGate, InspectionFailure, FrameDecoder, encode_event, user_text


def events(parts, tool=False):
    result=[encode_event('messageStart',{'role':'assistant'})]
    if tool:
        result.append(encode_event('contentBlockStart',{'contentBlockIndex':0,'start':{'toolUse':{'toolUseId':'t1','name':'book'}}}))
    for part in parts:
        result.append(encode_event('contentBlockDelta',{'contentBlockIndex':0,'delta':{'toolUse':{'input':part}} if tool else {'text':part}}))
    result += [encode_event('contentBlockStop',{'contentBlockIndex':0}), encode_event('messageStop',{'stopReason':'tool_use' if tool else 'end_turn'}),encode_event('metadata',{'usage':{'inputTokens':1,'outputTokens':1,'totalTokens':2}})]
    return result


class FakeScanner:
    def __init__(self, block=None, fail=False):
        self.settings=Settings(batch_chars=4)
        self.calls=[];self.block=block;self.fail=fail
    async def scan(self,text,direction,transaction):
        self.calls.append((text,direction))
        if self.fail: raise InspectionFailure(503,'unavailable')
        if self.block and self.block in text: raise InspectionFailure(403,'blocked')


def collect(frames, scanner, mode='batch', fragment=False):
    async def run():
        settings=Settings(batch_chars=4,mode=mode)
        gate=StreamGate(scanner,settings,'test');released=[]
        for raw in frames:
            for data in ([raw[i:i+3] for i in range(0,len(raw),3)] if fragment else [raw]):
                async for approved in gate.feed(data): released.append(approved)
        gate.finish()
        return released
    return asyncio.run(run())


def test_allow_preserves_frames_across_split_network_bytes():
    frames=events(['abcd','ef']);scanner=FakeScanner()
    assert collect(frames,scanner,fragment=True)==frames
    assert scanner.calls==[('abcd','OUT'),('abcdef','OUT')]


def test_full_mode_scans_once_before_any_release():
    frames=events(['abcd','ef']);scanner=FakeScanner()
    assert collect(frames,scanner,mode='full')==frames
    assert scanner.calls==[('abcdef','OUT')]


def test_blocked_first_batch_never_yields_content():
    scanner=FakeScanner(block='BAD!')
    async def run():
        gate=StreamGate(scanner,Settings(batch_chars=4),'t');released=[]
        with pytest.raises(InspectionFailure):
            for raw in events(['BAD!']):
                async for part in gate.feed(raw): released.append(part)
        assert released==[]
    asyncio.run(run())


def test_later_block_holds_new_batch_and_scans_cumulative_prefix():
    scanner=FakeScanner(block='abcdBAD!')
    async def run():
        gate=StreamGate(scanner,Settings(batch_chars=4),'t');released=[]
        with pytest.raises(InspectionFailure):
            for raw in events(['abcd','BAD!']):
                async for part in gate.feed(raw): released.append(part)
        assert b'abcd' in b''.join(released) and b'BAD!' not in b''.join(released)
        assert scanner.calls==[('abcd','OUT'),('abcdBAD!','OUT')]
    asyncio.run(run())


def test_tool_fragments_held_until_complete_and_scanned_together():
    scanner=FakeScanner();frames=events(['{"date":','"tomorrow"}'],tool=True)
    assert collect(frames,scanner)==frames
    assert len(scanner.calls)==1
    assert 'book' in scanner.calls[0][0] and '\\"tomorrow\\"' in scanner.calls[0][0]


@pytest.mark.parametrize('corrupt', ['crc','truncated','missing_stop','reasoning','unknown'])
def test_invalid_or_unsupported_stream_fails_closed(corrupt):
    frames=events(['ok'])
    if corrupt=='crc':
        damaged=bytearray(frames[1]);damaged[-1]^=1;frames[1]=bytes(damaged)
    elif corrupt=='truncated': frames[-1]=frames[-1][:-1]
    elif corrupt=='missing_stop': frames=frames[:2]
    elif corrupt=='reasoning': frames[1]=encode_event('contentBlockDelta',{'contentBlockIndex':0,'delta':{'reasoningContent':{'text':'thinking'}}})
    elif corrupt=='unknown': frames[1]=encode_event('unknownEvent',{'text':'bad'})
    with pytest.raises(InspectionFailure): collect(frames,FakeScanner(),mode='full')


def test_size_caps_fail_closed():
    async def run():
        gate=StreamGate(FakeScanner(),Settings(batch_chars=4,max_content_bytes=3),'t')
        with pytest.raises(InspectionFailure):
            for raw in events(['abcd']):
                async for _ in gate.feed(raw): pass
        decoder=FrameDecoder(100)
        with pytest.raises(InspectionFailure): list(decoder.feed(events(['long'])[0]))
    asyncio.run(run())


def test_input_excludes_system_assistant_tools_and_tool_results():
    body={'system':[{'text':'PRIVATE_SYSTEM'}],'messages':[{'role':'user','content':[{'text':'Book for two'}]},{'role':'assistant','content':[{'toolUse':{'name':'lookup','input':{}}}]},{'role':'user','content':[{'toolResult':{'content':[{'text':'SECRET_TOOL_RESULT'}]}}]}]}
    assert user_text(body)=='Book for two'
    with pytest.raises(InspectionFailure): user_text({'messages':[{'role':'user','content':[{'image':{}}]}]})


@pytest.mark.parametrize('http_status,verdict,expected',[
    (200,{'statusCode':200,'action':'ALLOW'},None),
    (200,{'statusCode':200,'action':'DETECT'},None),
    (200,{'statusCode':200,'action':'BLOCK'},403),
    (200,{'statusCode':500,'action':'ALLOW','errorMsg':'All detectors are either not found or had execution errors'},None),
    (200,{'action':'ALLOW'},None),
    (429,{'statusCode':429},503),
    (500,{},503),
    (200,{'statusCode':200,'action':'ALLOW','throttlingDetails':{'metric':'cs','retryAfterMillis':100}},503),
    (200,{'statusCode':200,'action':'ALLOW','throttlingDetails':{'metric':'rq','retryAfterMillis':100}},503),
    (201,{'statusCode':200,'action':'ALLOW'},503),
    (200,{'action':'UNKNOWN'},503),
])
def test_das_contract(monkeypatch,http_status,verdict,expected):
    monkeypatch.setenv('AIGUARD_URL','https://guard.invalid/execute-policy');monkeypatch.setenv('AIGUARD_API_KEY','fake');monkeypatch.setenv('AIGUARD_POLICY_ID','100')
    async def run():
        scanner=DASScanner();await scanner.client.aclose();seen=[]
        def handle(request):
            seen.append(json.loads(request.content));assert request.headers['authorization']=='Bearer fake'
            return httpx.Response(http_status,json=verdict)
        scanner.client=httpx.AsyncClient(transport=httpx.MockTransport(handle))
        try:
            if expected:
                with pytest.raises(InspectionFailure) as caught: await scanner.scan('text','OUT','t')
                assert caught.value.status==expected
            else: await scanner.scan('text','OUT','t')
            assert seen==[{'content':'text','direction':'OUT','policyId':100,'transactionId':'t'}]
        finally: await scanner.close()
    asyncio.run(run())


def test_native_route_on_real_http_server(monkeypatch,tmp_path):
    """Real socket + real LiteLLM route/config/hooks; AWS provider and DAS mocked."""
    import native_guard
    import litellm, uvicorn
    from litellm.proxy import proxy_server as ps
    from litellm.proxy.auth.user_api_key_auth import user_api_key_auth
    from litellm.proxy._types import UserAPIKeyAuth
    monkeypatch.setenv('AWS_REGION_NAME','us-east-1')
    monkeypatch.setenv('AIGUARD_URL','https://guard.invalid/execute-policy');monkeypatch.setenv('AIGUARD_API_KEY','fake');monkeypatch.setenv('AIGUARD_POLICY_ID','100')
    scanner=DASScanner();asyncio.run(scanner.client.aclose());scanner.settings.batch_chars=4
    scenario={'parts':['abcd','ef'],'closed':False,'provider_calls':0,'scans':[],'count_at_first_release':None,'sent':0,'error':False,'disconnect':False}
    def guard_response(request):
        payload=json.loads(request.content);scenario['scans'].append(payload)
        action='BLOCK' if 'BAD!' in payload['content'] else 'ALLOW'
        return httpx.Response(200,json={'statusCode':200,'action':action})
    scanner.client=httpx.AsyncClient(transport=httpx.MockTransport(guard_response));native_guard._scanner=scanner
    callback_path=pathlib.Path(__file__).with_name('native_input.py')
    (tmp_path/'native_input.py').write_text(callback_path.read_text())
    config=tmp_path/'config.yaml'
    config.write_text('model_list:\n  - model_name: test-bedrock\n    litellm_params:\n      model: bedrock/anthropic.claude-3-5-sonnet-20241022-v2:0\n      aws_region_name: us-east-1\nlitellm_settings:\n  callbacks:\n    - native_input.proxy_handler_instance\n')
    router,models,settings=asyncio.run(ps.proxy_config.load_config(None,str(config)))
    ps.llm_router=router;ps.general_settings=settings;ps.premium_user=False;ps.proxy_logging_obj.premium_user=False
    async def auth(): return UserAPIKeyAuth(api_key='test',user_role='proxy_admin')
    ps.app.dependency_overrides[user_api_key_auth]=auth
    async def provider(**kwargs):
        scenario['provider_calls']+=1
        scenario['request_body']=kwargs['data']
        async def stream():
            try:
                for index,raw in enumerate(events(scenario['parts'])):
                    scenario['sent']+=1
                    yield raw
                    await asyncio.sleep(0.025)
                    if scenario['error'] and index==1: raise RuntimeError('mock upstream error')
            finally: scenario['closed']=True
        return stream()
    monkeypatch.setattr(router,'allm_passthrough_route',provider)
    from converse_app import app
    with socket.socket() as sock:
        sock.bind(('127.0.0.1',0));port=sock.getsockname()[1]
    server=uvicorn.Server(uvicorn.Config(app,host='127.0.0.1',port=port,lifespan='off',log_level='critical'))
    thread=threading.Thread(target=server.run,daemon=True);thread.start()
    deadline=time.monotonic()+10
    while not server.started and time.monotonic()<deadline: time.sleep(.01)
    assert server.started
    base=f'http://127.0.0.1:{port}'
    path='/bedrock/model/test-bedrock/converse-stream'
    body={'system':[{'text':'PRIVATE_SYSTEM'}],'messages':[{'role':'user','content':[{'text':'Book for two'}]}]}
    report=[]
    def reset(parts,mode='batch',error=False):
        scenario.update(parts=parts,closed=False,provider_calls=0,scans=[],sent=0,error=error)
        scanner.settings.mode=mode
    try:
        with httpx.Client(base_url=base,timeout=5) as client:
            reset(['abcd','ef'])
            with client.stream('POST',path,json=body) as response:
                raw=b'';first=None
                for chunk in response.iter_bytes():
                    if first is None: first=scenario['sent']
                    raw+=chunk
                assert response.status_code==200 and raw==b''.join(events(['abcd','ef']))
                assert first<len(events(['abcd','ef']))  # Released before provider completed.
            assert scenario['request_body']==body
            assert scenario['scans'][0]['direction']=='IN' and scenario['scans'][0]['content']=='Book for two'
            assert [p['content'] for p in scenario['scans'] if p['direction']=='OUT']==['abcd','abcdef']
            report.append({'case':'allowed batch','result':'PASS','first_release_before_upstream_complete':True})
            reset(['BAD!'])
            response=client.post(path,json=body)
            assert response.status_code==403 and b'BAD!' not in response.content and scenario['closed']
            report.append({'case':'first batch blocked','result':'PASS','status':403,'upstream_closed':True})
            reset(['abcd','BAD!'])
            response=client.post(path,json=body)
            parser=EventStreamBuffer();parser.add_data(response.content);parsed=list(parser)
            assert response.status_code==200 and b'abcd' in response.content and b'BAD!' not in response.content
            assert parsed[-1].headers.get(':exception-type')=='internalServerException' and scenario['closed']
            report.append({'case':'later batch blocked','result':'PASS','native_exception':True,'blocked_text_withheld':True})
            reset(['abcd','BAD!'],'full')
            response=client.post(path,json=body)
            assert response.status_code==403 and b'abcd' not in response.content and b'BAD!' not in response.content
            report.append({'case':'full mode blocked','result':'PASS','no_output_released':True})
            reset(['abcd','ef'],'full',error=True)
            response=client.post(path,json=body)
            assert response.status_code==503 and b'abcd' not in response.content and scenario['closed']
            report.append({'case':'upstream failure before release','result':'PASS','status':503})
            reset(['abcd','ef'],error=True)
            response=client.post(path,json=body)
            parser=EventStreamBuffer();parser.add_data(response.content);parsed=list(parser)
            assert parsed[-1].headers.get(':exception-type')=='internalServerException' and scenario['closed']
            report.append({'case':'upstream failure after release','result':'PASS','native_exception':True})
            reset(['abcd','ef'])
            blocked_body={'messages':[{'role':'user','content':[{'text':'BAD!'}]}]}
            response=client.post(path,json=blocked_body)
            assert response.status_code==403 and scenario['provider_calls']==0
            report.append({'case':'input blocked','result':'PASS','provider_calls':0})
            reset(['abcd']*100)
            with client.stream('POST',path,json=body) as response:
                iterator=response.iter_bytes();next(iterator)
            deadline=time.monotonic()+3
            while not scenario['closed'] and time.monotonic()<deadline: time.sleep(.01)
            assert scenario['closed'] and scenario['sent']<len(events(['abcd']*100))
            report.append({'case':'client disconnect','result':'PASS','upstream_closed':True})
        assert importlib.metadata.version('litellm')=='1.103.0' and not os.getenv('LITELLM_LICENSE') and not ps.proxy_logging_obj.premium_user
        pathlib.Path(__file__).with_name('http_test_results.json').write_text(json.dumps({'litellm_version':'1.103.0','enterprise_license':False,'boundary':'real HTTP server and LiteLLM native route; provider method and DAS HTTP transport mocked; proxy auth dependency overridden for test','cases':report},indent=2)+'\n')
    finally:
        server.should_exit=True;thread.join(timeout=5)
        asyncio.run(scanner.close())
        ps.app.dependency_overrides.pop(user_api_key_auth,None)


def test_no_release_while_guard_verdict_is_pending():
    async def run():
        verdict=asyncio.Event();inspecting=asyncio.Event();released=[]
        class DelayedScanner(FakeScanner):
            async def scan(self,text,direction,transaction):
                inspecting.set();await verdict.wait()
        gate=StreamGate(DelayedScanner(),Settings(batch_chars=4),'t')
        async def producer():
            for raw in events(['abcd']):
                async for part in gate.feed(raw):released.append(part)
            gate.finish()
        task=asyncio.create_task(producer())
        await inspecting.wait()
        assert released==[] and not task.done()
        verdict.set();await task
        assert released==events(['abcd'])
    asyncio.run(run())


def test_parallel_requests_do_not_share_accumulated_text():
    async def run():
        async def one(text):
            scanner=FakeScanner();gate=StreamGate(scanner,Settings(batch_chars=4),'t');released=[]
            for raw in events([text]):
                async for part in gate.feed(raw):released.append(part)
                await asyncio.sleep(0)
            gate.finish()
            assert all(value==text for value,direction in scanner.calls)
            assert released==events([text])
        await asyncio.gather(*(one('request-'+str(i)) for i in range(12)))
    asyncio.run(run())
