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
from crawl_framework.sites.kabutan import (
    KabutanPlugin,
)
from crawl_framework.sites.hkexnews import (
    HKEXNewsPlugin,
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

    kabutan = registry.create(
        "kabutan"
    )

    assert isinstance(
        kabutan,
        KabutanPlugin,
    )

    assert kabutan.site_id == "kabutan"
    assert kabutan.country == "JP"
    assert kabutan.timezone == "Asia/Tokyo"
    assert kabutan.datasets() == ("news_article",)

    hkexnews = registry.create(
        "hkexnews"
    )

    assert isinstance(hkexnews, HKEXNewsPlugin)
    assert hkexnews.site_id == "hkexnews"
    assert hkexnews.country == "HK"
    assert hkexnews.timezone == "Asia/Hong_Kong"
    assert hkexnews.datasets() == (
        "financial_report",
        "financial_report_instrument",
        "attachment",
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
            "hkexnews",
            "kabutan",
            "naver_finance",
            "tossinvest",
        )
    )
