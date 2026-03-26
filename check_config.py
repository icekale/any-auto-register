import sys
import os
from sqlmodel import Session, select

# 添加当前目录到路径
sys.path.insert(0, os.path.dirname(__file__))

from core.db import engine
from core.config_store import ConfigItem

def check_configs():
    with Session(engine) as session:
        configs = session.exec(select(ConfigItem)).all()
        print("=== 当前全局配置 ===")
        for item in configs:
            # 遮蔽敏感数据
            val = item.value
            if "key" in item.key.lower() or "token" in item.key.lower():
                val = val[:4] + "***" + val[-4:] if len(val) > 8 else "***"
            print(f"{item.key:30} : {val}")

if __name__ == "__main__":
    check_configs()
