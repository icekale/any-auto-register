"""手动测试批量删除功能"""
import sys
import json
from core.db import init_db, AccountModel, engine
from sqlmodel import Session, select

def test_batch_delete():
    """测试批量删除端点"""
    print("初始化数据库...")
    init_db()
    
    # 创建测试账号
    print("\n创建测试账号...")
    with Session(engine) as session:
        acc1 = AccountModel(platform="test", email="test1@example.com", password="pass1")
        acc2 = AccountModel(platform="test", email="test2@example.com", password="pass2")
        acc3 = AccountModel(platform="test", email="test3@example.com", password="pass3")
        session.add(acc1)
        session.add(acc2)
        session.add(acc3)
        session.commit()
        session.refresh(acc1)
        session.refresh(acc2)
        session.refresh(acc3)
        
        test_ids = [acc1.id, acc2.id, acc3.id]
        print(f"创建了账号 IDs: {test_ids}")
    
    # 测试批量删除 API
    print("\n测试批量删除 API...")
    from fastapi.testclient import TestClient
    from main import app
    
    client = TestClient(app)
    
    # 测试 1: 空列表
    print("\n测试 1: 空列表应返回 400")
    response = client.post("/api/accounts/batch-delete", json={"ids": []})
    print(f"状态码: {response.status_code}")
    print(f"响应: {response.json()}")
    assert response.status_code == 400, "空列表应返回 400"
    print("✓ 通过")
    
    # 测试 2: 超过 1000 个
    print("\n测试 2: 超过 1000 个应返回 400")
    response = client.post("/api/accounts/batch-delete", json={"ids": list(range(1001))})
    print(f"状态码: {response.status_code}")
    print(f"响应: {response.json()}")
    assert response.status_code == 400, "超过 1000 个应返回 400"
    print("✓ 通过")
    
    # 测试 3: 成功删除
    print("\n测试 3: 成功删除前两个账号")
    response = client.post("/api/accounts/batch-delete", json={"ids": [test_ids[0], test_ids[1]]})
    print(f"状态码: {response.status_code}")
    data = response.json()
    print(f"响应: {json.dumps(data, indent=2, ensure_ascii=False)}")
    assert response.status_code == 200, "应返回 200"
    assert data["deleted"] == 2, f"应删除 2 个，实际删除 {data['deleted']}"
    assert data["not_found"] == [], f"不应有未找到的 ID，实际: {data['not_found']}"
    assert data["total_requested"] == 2, f"请求总数应为 2，实际: {data['total_requested']}"
    print("✓ 通过")
    
    # 验证删除成功
    print("\n验证账号已删除...")
    with Session(engine) as session:
        acc1_check = session.get(AccountModel, test_ids[0])
        acc2_check = session.get(AccountModel, test_ids[1])
        acc3_check = session.get(AccountModel, test_ids[2])
        assert acc1_check is None, "账号 1 应已删除"
        assert acc2_check is None, "账号 2 应已删除"
        assert acc3_check is not None, "账号 3 应仍存在"
        print("✓ 账号 1 和 2 已删除，账号 3 仍存在")
    
    # 测试 4: 部分 ID 不存在
    print("\n测试 4: 部分 ID 不存在")
    response = client.post("/api/accounts/batch-delete", json={"ids": [test_ids[2], 999, 1000]})
    print(f"状态码: {response.status_code}")
    data = response.json()
    print(f"响应: {json.dumps(data, indent=2, ensure_ascii=False)}")
    assert response.status_code == 200, "应返回 200"
    assert data["deleted"] == 1, f"应删除 1 个，实际删除 {data['deleted']}"
    assert set(data["not_found"]) == {999, 1000}, f"未找到的 ID 应为 [999, 1000]，实际: {data['not_found']}"
    assert data["total_requested"] == 3, f"请求总数应为 3，实际: {data['total_requested']}"
    print("✓ 通过")
    
    # 测试 5: 所有 ID 都不存在
    print("\n测试 5: 所有 ID 都不存在")
    response = client.post("/api/accounts/batch-delete", json={"ids": [999, 1000, 1001]})
    print(f"状态码: {response.status_code}")
    data = response.json()
    print(f"响应: {json.dumps(data, indent=2, ensure_ascii=False)}")
    assert response.status_code == 200, "应返回 200"
    assert data["deleted"] == 0, f"应删除 0 个，实际删除 {data['deleted']}"
    assert set(data["not_found"]) == {999, 1000, 1001}, f"未找到的 ID 应为 [999, 1000, 1001]，实际: {data['not_found']}"
    assert data["total_requested"] == 3, f"请求总数应为 3，实际: {data['total_requested']}"
    print("✓ 通过")
    
    print("\n" + "="*50)
    print("所有测试通过！✓")
    print("="*50)

if __name__ == "__main__":
    try:
        test_batch_delete()
    except Exception as e:
        print(f"\n✗ 测试失败: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
