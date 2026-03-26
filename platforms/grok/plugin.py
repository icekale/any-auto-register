"""Grok (x.ai) 平台插件"""
from core.base_platform import BasePlatform, Account, AccountStatus, RegisterConfig
from core.base_mailbox import BaseMailbox
from core.registry import register


@register
class GrokPlatform(BasePlatform):
    name = "grok"
    display_name = "Grok"
    version = "1.0.0"

    def __init__(self, config: RegisterConfig = None, mailbox: BaseMailbox = None):
        super().__init__(config)
        self.mailbox = mailbox

    def register(self, email: str, password: str = None) -> Account:
        from platforms.grok.core import GrokRegister
        from core.config_store import config_store
        log = getattr(self, '_log_fn', print)

        mail_acct = self.mailbox.get_email() if self.mailbox else None
        email = email or (mail_acct.email if mail_acct else None)
        log(f"邮箱: {email}")
        before_ids = self.mailbox.get_current_ids(mail_acct) if mail_acct else set()

        def otp_cb():
            log("等待验证码...")
            code = self.mailbox.wait_for_code(mail_acct, keyword="", before_ids=before_ids,
                                              code_pattern=r'[A-Z0-9]{3}-[A-Z0-9]{3}')
            if code:
                # 清洗验证码：移除连字符和空格 (API 通常期望纯字符)
                code = code.replace('-', '').replace(' ', '')
                log(f"验证码: {code}")
            return code

        # 优先从任务配置读取，兜底从全局配置读取
        yescaptcha_key = self.config.extra.get("yescaptcha_key") or config_store.get("yescaptcha_key", "")
        reg = GrokRegister(
            yescaptcha_key=yescaptcha_key,
            proxy=self.config.proxy,
            log_fn=log,
        )
        result = reg.register(
            email=email,
            password=password,
            otp_callback=otp_cb if self.mailbox else None,
        )

        return Account(
            platform="grok",
            email=result["email"],
            password=result["password"],
            status=AccountStatus.REGISTERED,
            extra={
                "sso": result["sso"],
                "sso_rw": result["sso_rw"],
                "given_name": result["given_name"],
                "family_name": result["family_name"],
            },
        )

    def check_valid(self, account: Account) -> bool:
        return bool((account.extra or {}).get("sso"))

    def get_platform_actions(self) -> list:
        return []

    def execute_action(self, action_id: str, account: Account, params: dict) -> dict:
        raise NotImplementedError(f"未知操作: {action_id}")
