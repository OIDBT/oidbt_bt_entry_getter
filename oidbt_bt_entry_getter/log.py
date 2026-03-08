from easyrip.easyrip_log import log
from easyrip.easyrip_mlang import Global_lang_val, get_system_language

Global_lang_val.gettext_target_lang = get_system_language()
log.write_level = log.LogLevel.none

log.init()
