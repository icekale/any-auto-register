"""测试批量删除功能"""
import pytest
from fastapi.testclient import TestClient
from sqlmodel import Session, create_engine, SQLModel
from sqlmodel.pool import StaticPool
from core.db import AccountModel, get_session
from main import app

# 创建测试数据库
@pytest.fixture(name="session")
def session_fixture():
    engine = create_engine(
        "sqlite:///:memory:",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    SQLModel.metadata.create_all(engine)
    with Session(engine) as session:
        yield session


@pytest.fixture(name="client")
def client_fixture(session: Session):
    def get_session_override():
        return session

    app.dependency_overrides[get_session] = get_session_override
    client = TestClient(app)
    yield client
    app.dependency_overrides.clear()


def test_batch_delete_empty_list(client: TestClient):
    """测试空列表返回 400"""
    response = client.post("/api/accounts/batch-delete", json={"ids": []})
    assert response.status_code == 400
    assert "不能为空" in response.json()["detail"]


def test_batch_delete_too_many(client: TestClient):
    """测试超过 1000 个返回 400"""
    response = client.post("/api/accounts/batch-delete", json={"ids": list(range(1001))})
    assert response.status_code == 400
    assert "1000" in response.json()["detail"]


def test_batch_delete_success(client: TestClient, session: Session):
    """测试成功删除"""
    # 创建测试账号
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

    # 批量删除
    response = client.post("/api/accounts/batch-delete", json={"ids": [acc1.id, acc2.id]})
    assert response.status_code == 200
    data = response.json()
    assert data["deleted"] == 2
    assert data["not_found"] == []
    assert data["total_requested"] == 2

    # 验证删除成功
    assert session.get(AccountModel, acc1.id) is None
    assert session.get(AccountModel, acc2.id) is None
    assert session.get(AccountModel, acc3.id) is not None


def test_batch_delete_partial(client: TestClient, session: Session):
    """测试部分 ID 不存在"""
    # 创建测试账号
    acc1 = AccountModel(platform="test", email="test1@example.com", password="pass1")
    session.add(acc1)
    session.commit()
    session.refresh(acc1)

    # 批量删除（包含不存在的 ID）
    response = client.post("/api/accounts/batch-delete", json={"ids": [acc1.id, 999, 1000]})
    assert response.status_code == 200
    data = response.json()
    assert data["deleted"] == 1
    assert set(data["not_found"]) == {999, 1000}
    assert data["total_requested"] == 3

    # 验证删除成功
    assert session.get(AccountModel, acc1.id) is None


def test_batch_delete_all_not_found(client: TestClient):
    """测试所有 ID 都不存在"""
    response = client.post("/api/accounts/batch-delete", json={"ids": [999, 1000, 1001]})
    assert response.status_code == 200
    data = response.json()
    assert data["deleted"] == 0
    assert set(data["not_found"]) == {999, 1000, 1001}
    assert data["total_requested"] == 3


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
