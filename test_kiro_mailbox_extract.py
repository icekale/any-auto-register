from core.base_mailbox import BaseMailbox, MailboxAccount


class _DummyMailbox(BaseMailbox):
    def get_email(self) -> MailboxAccount:
        return MailboxAccount(email="dummy@example.com")

    def wait_for_code(self, account: MailboxAccount, keyword: str = "",
                      timeout: int = 120, before_ids: set = None,
                      code_pattern: str = None, **kwargs) -> str:
        raise NotImplementedError

    def get_current_ids(self, account: MailboxAccount) -> set:
        return set()


RAW_AWS_MAIL = """DKIM-Signature: v=1
Content-Type: multipart/alternative;
 boundary="----=_Part_328392_651279443.1774544433427"

------=_Part_328392_651279443.1774544433427
Content-Type: text/plain; charset=UTF-8
Content-Transfer-Encoding: 7bit

Verify your AWS Builder ID email address

Verification code:: 635521

This code will expire 30 minutes after it was sent.
------=_Part_328392_651279443.1774544433427--
"""


def test_safe_extract_prefers_semantic_code_over_mime_boundary():
    mailbox = _DummyMailbox()
    decoded = mailbox._decode_raw_content(RAW_AWS_MAIL)

    assert "328392" not in decoded
    assert mailbox._safe_extract(decoded) == "635521"


def test_safe_extract_supports_kiro_specific_pattern():
    mailbox = _DummyMailbox()
    decoded = mailbox._decode_raw_content(RAW_AWS_MAIL)
    pattern = r"(?is)(?:verification\\s+code|验证码)[^0-9]{0,20}(\\d{6})"

    assert mailbox._safe_extract(decoded, pattern) == "635521"
