from aiohttp import web

from dtps_http_programs import get_clock_app as get_app

if __name__ == '__main__':
    app = get_app()
    web.run_app(app)
