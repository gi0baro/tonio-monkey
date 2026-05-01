async def app(scope, proto):
    headers = [("content-type", scope.headers.get("content-type", "text/plain; charset=utf-8"))]
    trx = proto.response_stream(200, headers)
    async for chunk in proto:
        await trx.send_bytes(chunk)
