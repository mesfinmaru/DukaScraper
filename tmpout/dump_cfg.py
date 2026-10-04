import json, sys
from camoufox.utils import launch_options, Screen
c = None if sys.argv[1] == "base" else {"timezone": "Africa/Nairobi"}
import io, contextlib
buf = io.StringIO()
with contextlib.redirect_stdout(buf):
    launch_options(headless=False, humanize=True, screen=Screen(max_width=1920, max_height=1080),
                   window=(1920, 1040), os="windows", locale="en-US", config=c,
                   i_know_what_im_doing=True, debug=True)
txt = buf.getvalue()
start = txt.find("{")
end = txt.rfind("}")
cfg = eval(txt[start:end + 1])  # debug output is a python dict repr
cfg.pop("fonts", None)
json.dump(cfg, open(f"/tmp/cfg_{sys.argv[1]}.json", "w"), indent=1, default=str, sort_keys=True)
print("keys:", len(cfg))
