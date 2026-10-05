"""Локальный рендер диаграмм: страница рисует Mermaid в браузере и POST-ит PNG обратно на этот сервер."""
import base64
import http.server
import json
import pathlib
import sys

sys.path.insert(0, str(pathlib.Path(__file__).parent))
import make_diagrams  # noqa: E402

HERE = pathlib.Path(__file__).parent
OUT = HERE / "png"
OUT.mkdir(exist_ok=True)

PAGE = """<!doctype html><html><head><meta charset="utf-8"><title>render</title>
<script src="https://cdn.jsdelivr.net/npm/mermaid@11/dist/mermaid.min.js"></script>
<style>body{background:#fff;font-family:Arial} .d{margin:20px}</style></head><body>
<div id="log">rendering...</div><div id="host"></div>
<script>
const DIAGRAMS = __DATA__;
mermaid.initialize({startOnLoad:false, theme:'default', htmlLabels:false, flowchart:{htmlLabels:false, curve:'basis'},
                    themeVariables:{fontFamily:'Arial'}, securityLevel:'loose'});
async function toPng(svgText, scale){
  const host = document.getElementById('host'); host.innerHTML = svgText;
  const svg = host.querySelector('svg'); const vb = svg.viewBox.baseVal;
  const w = Math.ceil(vb.width), h = Math.ceil(vb.height);
  svg.setAttribute('width', w); svg.setAttribute('height', h);
  const xml = new XMLSerializer().serializeToString(svg);
  const img = new Image();
  await new Promise((ok, err) => { img.onload = ok; img.onerror = err;
    img.src = 'data:image/svg+xml;base64,' + btoa(unescape(encodeURIComponent(xml))); });
  const c = document.createElement('canvas'); c.width = w*scale; c.height = h*scale;
  const ctx = c.getContext('2d'); ctx.fillStyle = '#fff'; ctx.fillRect(0,0,c.width,c.height);
  ctx.scale(scale, scale); ctx.drawImage(img, 0, 0);
  return c.toDataURL('image/png');
}
(async () => {
  const done = [];
  for (const [name, src] of Object.entries(DIAGRAMS)) {
    try {
      const {svg} = await mermaid.render('m_' + name.replace(/[^a-z0-9]/gi,'_'), src.replace(/<\/?b>/g,''));
      const png = await toPng(svg, 2);
      await fetch('/save?name=' + name, {method:'POST', body: png});
      done.push(name + ':ok');
    } catch (e) { done.push(name + ':ERR ' + e); }
  }
  document.getElementById('log').textContent = 'DONE ' + done.join(' | ');
  window.RESULT = done;
})();
</script></body></html>"""


class H(http.server.BaseHTTPRequestHandler):
    def do_GET(self):
        body = PAGE.replace("__DATA__", json.dumps(make_diagrams.D, ensure_ascii=False)).encode()
        self.send_response(200)
        self.send_header("content-type", "text/html; charset=utf-8")
        self.end_headers()
        self.wfile.write(body)

    def do_POST(self):
        name = self.path.split("name=")[1]
        data = self.rfile.read(int(self.headers["Content-Length"])).decode()
        (OUT / f"{name}.png").write_bytes(base64.b64decode(data.split(",", 1)[1]))
        print("saved", name, flush=True)
        self.send_response(200)
        self.end_headers()

    def log_message(self, *a):
        pass


http.server.HTTPServer(("127.0.0.1", 8765), H).serve_forever()
