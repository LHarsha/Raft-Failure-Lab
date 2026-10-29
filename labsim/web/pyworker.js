"use strict";
/* Runs the Python simulator in the browser for the static (GitHub Pages) build.
   Loads Pyodide, copies the labsim sources listed in py/manifest.json into its
   file system, then answers requests by calling the same ui.handle() the local
   server uses, so results are identical to `python -m labsim ui`. */
const PYODIDE = "https://cdn.jsdelivr.net/pyodide/v0.26.4/full/";
importScripts(PYODIDE + "pyodide.js");

let ready = null;

async function boot(base) {
  const py = await loadPyodide({ indexURL: PYODIDE });
  const manifest = await (await fetch(base + "manifest.json")).json();
  py.FS.mkdirTree("/lab/labsim");
  for (const f of manifest.files) {
    const r = await fetch(base + f);
    if (!r.ok) throw new Error(`could not load ${f} (${r.status})`);
    py.FS.writeFile("/lab/" + f, await r.text());
  }
  py.runPython("import sys, json\nsys.path.insert(0, '/lab')\nfrom labsim import ui");
  return py;
}

self.onmessage = async ({ data }) => {
  if (data.type === "init") {
    ready = boot(data.base);
    try { await ready; postMessage({ type: "ready" }); }
    catch (e) { postMessage({ type: "error", error: String(e) }); }
    return;
  }
  try {
    const py = await ready;
    py.globals.set("REQ", data.path);
    const out = py.runPython("_s, _c, _b = ui.handle(REQ)\njson.dumps([_s, _b.decode('utf-8')])");
    const [status, body] = JSON.parse(out);
    postMessage({ id: data.id, status, body });
  } catch (e) {
    postMessage({ id: data.id, status: 500, body: JSON.stringify({ error: String(e) }) });
  }
};
