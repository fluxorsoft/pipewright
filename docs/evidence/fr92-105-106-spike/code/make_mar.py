"""Build model.mar (a zip with MAR-INF/MANIFEST.json and the handler) without torch-model-archiver."""
import json, os, sys, zipfile

here = os.path.dirname(os.path.abspath(__file__))
out = sys.argv[1]
manifest = {"createdOn": "07/11/2026 20:00:00", "runtime": "python",
            "model": {"modelName": "model", "handler": "handler.py", "modelVersion": "1.0"},
            "archiverVersion": "0.12.0"}
with zipfile.ZipFile(out, "w", zipfile.ZIP_DEFLATED) as z:
    z.writestr("MAR-INF/MANIFEST.json", json.dumps(manifest, indent=2))
    z.write(os.path.join(here, "handler.py"), "handler.py")
print(out)
