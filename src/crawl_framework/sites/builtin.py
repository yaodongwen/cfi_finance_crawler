from __future__ import annotations

from crawl_framework.app_factory import (
    SiteFactoryRegistry,
)

from crawl_framework.sites.naver_finance import (
    NAVER_FINANCE_SITE_ID,
    NaverFinancePlugin,
)
from crawl_framework.sites.tossinvest import (
    TOSSINVEST_SITE_ID,
    TossInvestPlugin,
)


def register_builtin_sites(
    registry: SiteFactoryRegistry,
) -> None:
    """
    注册 framework 内置网站插件。

    所有正式 site plugin
    统一在这里注册。

    hotcopper / stockhouse 后续继续在这里添加。
    """

    if not registry.contains(
        NAVER_FINANCE_SITE_ID
    ):

        registry.register(
            NAVER_FINANCE_SITE_ID,
            NaverFinancePlugin,
        )

    if not registry.contains(
        TOSSINVEST_SITE_ID
    ):

        registry.register(
            TOSSINVEST_SITE_ID,
            TossInvestPlugin,
        )
