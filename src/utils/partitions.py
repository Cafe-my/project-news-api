from datetime import datetime

def format_partition_key(partition_key: datetime) -> str:
    y = partition_key.strftime("%Y-%m")
    d = partition_key.strftime("%d")
    
    return f"y={y}/d={d}"