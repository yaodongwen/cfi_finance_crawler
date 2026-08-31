from crawl_framework.app_factory import (
    SiteFactoryRegistry,
)

from crawl_framework.sites.builtin import (
    register_builtin_sites,
)

from crawl_framework.sites.naver_finance import (
    NaverFinancePlugin,
)
from crawl_framework.sites.tossinvest import (
    TossInvestPlugin,
)


def test_register_builtin_sites():

    registry = (
        SiteFactoryRegistry()
    )

    register_builtin_sites(
        registry
    )

    assert (
        registry.contains(
            "naver_finance"
        )
    )

    assert (
        registry.contains(
            "tossinvest"
        )
    )

    plugin = registry.create(
        "naver_finance"
    )

    assert isinstance(
        plugin,
        NaverFinancePlugin,
    )

    toss = registry.create(
        "tossinvest"
    )

    assert isinstance(
        toss,
        TossInvestPlugin,
    )


def test_builtin_registration_is_idempotent():

    registry = (
        SiteFactoryRegistry()
    )

    register_builtin_sites(
        registry
    )

    register_builtin_sites(
        registry
    )

    assert (
        registry.list_sites()
        == (
            "naver_finance",
            "tossinvest",
        )
    )
