import sys
import os
import re

# 添加当前目录到路径
sys.path.insert(0, os.path.dirname(__file__))

from core.base_mailbox import CFWorkerMailbox, MailboxAccount

def test_safe_extract():
    print("=== 测试 _safe_extract ===")
    mailbox = CFWorkerMailbox(api_url="http://test.com")
    
    # 测试1: 默认6位数字 (带捕获组)
    text1 = "Your code is 123456."
    res1 = mailbox._safe_extract(text1)
    print(f"默认正则: {res1} (预期: 123456)")
    assert res1 == "123456"
    
    # 测试2: Grok 格式 (无捕获组)
    text2 = "Grok verification code: ABC-DEF"
    res2 = mailbox._safe_extract(text2, pattern=r'[A-Z]{3}-[A-Z]{3}')
    print(f"无捕获组正则: {res2} (预期: ABC-DEF)")
    assert res2 == "ABC-DEF"
    
    # 测试3: 带捕获组的自定义正则
    text3 = "Code: [XYZ-789]"
    res3 = mailbox._safe_extract(text3, pattern=r'\[([A-Z0-9-]{7})\]')
    print(f"带捕获组正则: {res3} (预期: XYZ-789)")
    assert res3 == "XYZ-789"
    
    print("✓ _safe_extract 测试通过\n")

def test_decode_logic():
    print("=== 测试 _decode_raw_content (Fugle 风格) ===")
    mailbox = CFWorkerMailbox(api_url="http://test.com")
    
    # 测试 Quoted-Printable 解码
    # 'Your code =3D 123456' -> 'Your code = 123456'
    raw_qp = "Subject: Test\n\nYour code =3D 123456"
    decoded = mailbox._decode_raw_content(raw_qp)
    print(f"QP 解码: {decoded} (预期包含: code = 123456)")
    assert "code = 123456" in decoded
    
    # 测试 HTML 清洗和实体转换
    raw_html = "<html><body>Code is &lt;b&gt;987654&lt;/b&gt;</body></html>"
    decoded_html = mailbox._decode_raw_content(raw_html)
    print(f"HTML 清洗: {decoded_html} (预期: Code is 987654)")
    assert "987654" in decoded_html
    
    print("✓ _decode_raw_content 测试通过\n")

if __name__ == "__main__":
    try:
        test_safe_extract()
        test_decode_logic()
        print("ALL TESTS PASSED!")
    except Exception as e:
        print(f"TEST FAILED: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
