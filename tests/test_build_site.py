import json
import tempfile
import unittest
from pathlib import Path

import labsim
from labsim.build_site import build
from tests.test_timeline import parse


class BuildSiteTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.out = Path(self.tmp.name) / "_site"
        build(self.out)

    def tearDown(self):
        self.tmp.cleanup()

    def test_app_files(self):
        for f in ["index.html", "static/app.js", "static/app.css", "static/pyworker.js", ".nojekyll"]:
            self.assertTrue((self.out / f).is_file(), f)

    def test_index_uses_relative_paths(self):
        # GitHub Pages serves the site under /<repo>/, so absolute /static/ paths would break
        html = (self.out / "index.html").read_text(encoding="utf-8")
        self.assertIn('src="static/app.js"', html)
        self.assertIn('href="static/app.css"', html)
        self.assertNotIn('"/static/', html)
        p = parse(html)
        self.assertEqual((p.stack, p.errors), ([], []))

    def test_every_labsim_module_shipped_and_listed(self):
        manifest = json.loads((self.out / "py/manifest.json").read_text(encoding="utf-8"))
        modules = sorted(p.name for p in Path(labsim.__file__).parent.glob("*.py"))
        shipped = sorted(Path(f).name for f in manifest["files"] if f.startswith("labsim/"))
        self.assertEqual(shipped, modules)
        for f in manifest["files"]:
            self.assertTrue((self.out / "py" / f).is_file(), f)
        self.assertIn("FINDINGS.md", manifest["files"])

    def test_rebuild_replaces_old_output(self):
        stale = self.out / "stale.txt"
        stale.write_text("x")
        build(self.out)
        self.assertFalse(stale.exists())


if __name__ == "__main__":
    unittest.main()
