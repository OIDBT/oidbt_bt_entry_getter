from pathlib import Path

from easyrip.easyrip_log import log
from easyrip.easyrip_mlang import Global_lang_val, get_system_language

Global_lang_val.gettext_target_lang = get_system_language()
log.write_level = log.LogLevel.none

log.html_file = Path("OIDBT-log.html")

log.init()
