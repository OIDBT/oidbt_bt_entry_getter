def get_size_str(data: bytes):
    size = len(data)
    return f"{size / 1024:.1f} KiB" if size > 2048 else f"{size} B"
