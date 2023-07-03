from aiohttp import web

from dt_ps_http_standalone import get_clock_app as get_app

if __name__ == '__main__':
    app = get_app()
    web.run_app(app)
