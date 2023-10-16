"""

    dtps = DTPS.from_environment()

    DTPS_URL =
    DTPS_

    dtps = DTPS.from_environment()

    async with dtps.session() as session:
        session.create('...')
        session.publish('...')


    async for msg in session.subscribe('...'):
        ...

    while True:
        await session.push()



"""
