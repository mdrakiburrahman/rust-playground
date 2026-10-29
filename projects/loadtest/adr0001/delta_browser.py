"""Validate the generated report using an isolated local browser, never a shared profile."""
import argparse
from contextlib import contextmanager, ExitStack
from http.server import BaseHTTPRequestHandler, HTTPServer
import json
import threading
from urllib.parse import urlparse

from delta_benchmark import digest, save
from publication_gate import REPO


@contextmanager
def serve_report(report):
    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            if self.path == "/favicon.ico":
                self.send_response(204)
                self.end_headers()
                return
            if self.path != "/report.html":
                self.send_error(404)
                return
            body = report.read_bytes()
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, *_):
            pass

    server = HTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{server.server_port}/report.html"
    finally:
        server.shutdown()
        thread.join()
        server.server_close()


def main():
    from playwright.sync_api import sync_playwright

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--cohort", required=True)
    parser.add_argument("--url", help="optional existing loopback report server")
    args = parser.parse_args()
    directory = REPO / "target/delta-benchmark" / args.cohort
    report = REPO / "projects/loadtest/adr0001/report.html"
    console, requests, views = [], [], []
    with ExitStack() as stack:
        url = args.url or stack.enter_context(serve_report(report))
        origin = urlparse(url)
        assert origin.scheme == "http" and origin.hostname == "127.0.0.1"
        runtime = stack.enter_context(sync_playwright())
        browser = runtime.chromium.launch()
        stack.callback(browser.close)
        page = browser.new_page()
        page.on("pageerror", lambda error: console.append(str(error)))
        page.on("console", lambda message: console.append(message.text) if message.type == "error" else None)
        page.on("request", lambda request: requests.append(request.url))
        for name, width, height in (("desktop", 1440, 1000), ("mobile", 390, 844)):
            page.set_viewport_size({"width": width, "height": height})
            response = page.goto(url, wait_until="networkidle")
            assert response.status == 200
            assert response.body() == report.read_bytes()
            result = page.evaluate("""() => {
                const bars=[...document.querySelectorAll('.bar[data-value]')].map(e=>({
                    value:+e.dataset.value, maximum:+e.dataset.max,
                    percent:parseFloat(e.style.width),
                    declaredPercent:parseFloat(e.getAttribute('style').match(/width:([0-9.]+)/)[1]),
                    renderedPercent:e.getBoundingClientRect().width/e.parentElement.clientWidth*100
                }));
                return {width:innerWidth, documentWidth:document.documentElement.scrollWidth,
                    appendixCollapsed:!document.querySelector('#appendix').open,
                    linksValid:[...document.querySelectorAll('a[href^="#"]')].every(a=>!!document.getElementById(a.hash.slice(1))),
                    assets:document.querySelectorAll('script,iframe,img,link[rel="stylesheet"]').length, bars};
            }""")
            assert result["documentWidth"] <= width + 1, result
            assert result["appendixCollapsed"] and result["linksValid"] and result["assets"] == 0
            assert len(result["bars"]) == 6
            for bar in result["bars"]:
                assert abs(bar["renderedPercent"] - bar["percent"]) < .1, (name, bar)
                assert abs(bar["declaredPercent"] - 100 * bar["value"] / bar["maximum"]) < .000001
            screenshot = directory / f"browser-{name}.png"
            page.screenshot(path=str(screenshot), full_page=True)
            page.locator("#appendix > summary").click()
            assert page.locator("#appendix").evaluate("(e) => e.open")
            result["expandedDocumentWidth"] = page.evaluate("document.documentElement.scrollWidth")
            assert result["expandedDocumentWidth"] <= width + 1, result
            page.locator("#appendix > summary").click()
            assert not page.locator("#appendix").evaluate("(e) => e.open")
            result.update(view=name, screenshot=str(screenshot.relative_to(REPO)),
                          screenshot_sha256=digest(screenshot), toggle_verified=True)
            views.append(result)
        assert not console, console
        assert all(urlparse(url).netloc == origin.netloc for url in requests), requests
    result = {"passed": True, "sha256": digest(report), "views": views,
              "console_errors": console, "requests": requests,
              "scope": "Isolated local Chromium; MCP browser cannot save outside the primary worktree and timed out"}
    save(directory / "browser-validation.json", result)
    print(json.dumps({"passed": True, "views": len(views), "sha256": result["sha256"]}))


if __name__ == "__main__":
    main()
