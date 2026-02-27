from copy import deepcopy

from easyrip import log as _log

log = deepcopy(_log)

log.write_level = log.LogLevel.none
