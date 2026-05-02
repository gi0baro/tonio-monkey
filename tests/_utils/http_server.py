async def echo(scope, proto):
    headers = [("content-type", scope.headers.get("content-type", "text/plain; charset=utf-8"))]
    trx = proto.response_stream(200, headers)
    async for chunk in proto:
        await trx.send_bytes(chunk)


async def ws(scope, proto):
    trx = await proto.accept()

    while True:
        message = await trx.receive()
        if message.kind == 0:
            break
        elif message.kind == 1:
            await trx.send_bytes(message.data)
        else:
            await trx.send_str(message.data)

    proto.close()


def app(scope, proto):
    return {
        "/echo": echo,
        "/ws": ws,
    }[scope.path](scope, proto)
