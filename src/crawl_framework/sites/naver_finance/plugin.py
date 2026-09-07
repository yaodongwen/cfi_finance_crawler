from __future__ import annotations

from datetime import (
    datetime,
    timezone,
)
from typing import (
    Any,
    AsyncIterator,
)

from crawl_framework.core.models import (
    CanonicalRecord,
    RecordRelation,
)

from crawl_framework.core.plugin import (
    CrawlCheckpoint,
    CrawlContext,
    CrawlScope,
    SitePlugin,
)


# ============================================================
# Constants
# ============================================================


NAVER_FINANCE_SITE_ID = (
    "naver_finance"
)

NAVER_FINANCE_COUNTRY = "KR"

NAVER_FINANCE_TIMEZONE = (
    "Asia/Seoul"
)


# ============================================================
# Naver plugin
# ============================================================


class NaverFinancePlugin(
    SitePlugin
):
    """
    Naver Finance 网站插件。

    当前支持：

        forum_post
        news_article

    当前已经接入真实 forum_post crawler。

    SitePlugin 只负责：

        discover
        crawl
        normalize
        checkpoint

    不负责：

        SeenStore
        Parquet
        PostgreSQL
        Recovery
        Cleaner
        Upload

    这些由 framework 通用层负责。
    """

    site_id = NAVER_FINANCE_SITE_ID

    country = NAVER_FINANCE_COUNTRY

    timezone = NAVER_FINANCE_TIMEZONE


    # ========================================================
    # Init
    # ========================================================

    def __init__(
        self,
        *,
        forum_client=None,
        news_client=None,
        research_client=None,
        attachment_pipeline=None,
        rate_limiter=None,
        default_forum_max_pages: int | None = 1,
        default_news_max_pages: int | None = 1,
        default_research_max_pages: int | None = 1,
        fetch_forum_detail: bool = True,
        news_mode: str = "incremental",
        research_mode: str = "incremental",
    ) -> None:

        from crawl_framework.sites.naver_finance.forum_post import (
            NaverForumClient,
        )
        from crawl_framework.sites.naver_finance.news import (
            NaverNewsClient,
        )
        from crawl_framework.sites.naver_finance.research import (
            NaverResearchClient,
        )
        from crawl_framework.transports.rate_limit import (
            AdaptiveRateLimiter,
        )

        self.rate_limiter = (
            rate_limiter
            or AdaptiveRateLimiter()
        )

        self.forum_client = (
            forum_client
            or NaverForumClient(
                rate_limiter=self.rate_limiter,
            )
        )

        self.news_client = (
            news_client
            or NaverNewsClient(
                rate_limiter=self.rate_limiter,
            )
        )

        self.research_client = (
            research_client
            or NaverResearchClient(
                rate_limiter=self.rate_limiter,
            )
        )

        self.attachment_pipeline = attachment_pipeline

        if (
            default_forum_max_pages
            is not None
            and default_forum_max_pages <= 0
        ):

            raise ValueError(
                "default_forum_max_pages "
                "must be positive"
            )

        self.default_forum_max_pages = (
            default_forum_max_pages
        )

        if (
            default_news_max_pages
            is not None
            and default_news_max_pages <= 0
        ):

            raise ValueError(
                "default_news_max_pages "
                "must be positive"
            )

        self.default_news_max_pages = (
            default_news_max_pages
        )

        if (
            default_research_max_pages
            is not None
            and default_research_max_pages <= 0
        ):

            raise ValueError(
                "default_research_max_pages "
                "must be positive"
            )

        self.default_research_max_pages = (
            default_research_max_pages
        )

        self.fetch_forum_detail = (
            fetch_forum_detail
        )

        normalized_news_mode = str(
            news_mode
        ).strip().lower()

        if normalized_news_mode not in {
            "full",
            "incremental",
        }:

            raise ValueError(
                "news_mode must be full "
                "or incremental"
            )

        self.news_mode = (
            normalized_news_mode
        )

        normalized_research_mode = str(
            research_mode
        ).strip().lower()

        if normalized_research_mode not in {
            "full",
            "incremental",
        }:

            raise ValueError(
                "research_mode must be full "
                "or incremental"
            )

        self.research_mode = (
            normalized_research_mode
        )

    @staticmethod
    def _context_extra(
        context: CrawlContext | None,
    ) -> dict[
        str,
        Any,
    ]:
        """
        安全读取 CrawlContext.extra。

        Runtime 正式运行时传 CrawlContext；
        单元测试允许 context=None。
        """

        if context is None:

            return {}

        extra = getattr(
            context,
            "extra",
            None,
        )

        if not isinstance(
            extra,
            dict,
        ):

            return {}

        return extra
    # ========================================================
    # Datasets
    # ========================================================



    # ========================================================
    # Scope discovery
    # ========================================================


    # ========================================================
    # Normalize
    # ========================================================



    # ========================================================
    # Checkpoint
    # ========================================================

    def checkpoint_after_record(
        self,
        dataset: str,
        scope: CrawlScope,
        raw: dict[
            str,
            Any,
        ],
        record: CanonicalRecord,
        previous: CrawlCheckpoint | None,
        context: CrawlContext,
    ) -> CrawlCheckpoint:
        """
        每成功持久化一条记录后，
        生成新的 crawler checkpoint。

        当前 forum_post 保存：

            dataset
            source_key
            scope_id
            last_nid
            page

        news_article 保存：

            last_article_id
            last_office_id
        """

        state: dict[
            str,
            Any,
        ] = {}

        # ====================================================
        # Preserve previous state
        # ====================================================

        if (
            previous is not None
            and isinstance(
                previous.state,
                dict,
            )
        ):

            state.update(
                previous.state
            )

        # ====================================================
        # Common
        # ====================================================

        state[
            "dataset"
        ] = dataset

        state[
            "scope_type"
        ] = scope.scope_type

        state[
            "source_key"
        ] = scope.source_key

        if scope.scope_id:

            state[
                "scope_id"
            ] = scope.scope_id

        # ====================================================
        # Forum
        # ====================================================

        if dataset == "forum_post":

            nid = str(
                raw.get(
                    "nid"
                )
                or raw.get(
                    "source_id"
                )
                or record.source_id
            ).strip()

            if not nid:

                raise ValueError(
                    "cannot create forum_post "
                    "checkpoint without nid"
                )

            state[
                "last_nid"
            ] = nid

            page = raw.get(
                "page"
            )

            if page is not None:

                try:

                    page = int(
                        page
                    )

                except (
                    TypeError,
                    ValueError,
                ):

                    page = None

            if (
                page is not None
                and page > 0
            ):

                state[
                    "page"
                ] = page

        # ====================================================
        # News
        # ====================================================

        elif dataset in {
            "news_article",
            "news_instrument",
        }:

            article_id = str(
                raw.get(
                    "article_id"
                )
                or raw.get(
                    "source_id"
                )
                or record.source_id
            ).strip()

            if not article_id:

                raise ValueError(
                    "cannot create news_article "
                    "checkpoint without "
                    "article_id"
                )

            office_id = str(
                raw.get(
                    "office_id",
                    "",
                )
            ).strip()

            state[
                "last_article_id"
            ] = article_id

            if office_id:

                state[
                    "last_office_id"
                ] = office_id

            page = raw.get(
                "page"
            )

            if page is not None:

                try:

                    page = int(
                        page
                    )

                except (
                    TypeError,
                    ValueError,
                ):

                    page = None

            if (
                page is not None
                and page > 0
            ):

                state[
                    "page"
                ] = page

        elif dataset in {
            "research_report",
            "research_instrument",
        }:

            report_id = str(
                raw.get(
                    "report_id"
                )
                or raw.get(
                    "source_id"
                )
                or record.source_id
            ).strip()

            if not report_id:

                raise ValueError(
                    "cannot create research "
                    "checkpoint without report_id"
                )

            state[
                "last_report_id"
            ] = report_id

            category = self._optional_text(
                raw.get(
                    "category"
                )
                or raw.get(
                    "report_type"
                )
            )

            if category:

                state[
                    "category"
                ] = category

            page = raw.get(
                "page"
            )

            if page is not None:

                try:

                    page = int(
                        page
                    )

                except (
                    TypeError,
                    ValueError,
                ):

                    page = None

            if (
                page is not None
                and page > 0
            ):

                state[
                    "page"
                ] = page

        elif dataset == "attachment":

            attachment_id = str(
                raw.get(
                    "attachment_id"
                )
                or raw.get(
                    "sha256"
                )
                or record.source_id
            ).strip()

            if not attachment_id:

                raise ValueError(
                    "cannot create attachment "
                    "checkpoint without attachment_id"
                )

            state[
                "last_attachment_id"
            ] = attachment_id

            report_id = self._optional_text(
                raw.get(
                    "report_id"
                )
            )

            if report_id:

                state[
                    "last_report_id"
                ] = report_id

        # ====================================================
        # Fallback
        # ====================================================

        else:

            state[
                "last_source_id"
            ] = record.source_id

        return CrawlCheckpoint(
            state=state
        )


    # ========================================================
    # Forum normalize
    # ========================================================



    # ========================================================
    # News normalize
    # ========================================================



    # ========================================================
    # Forum checkpoint helpers
    # ========================================================

    def _forum_start_page(
        self,
        checkpoint: CrawlCheckpoint | None,
    ) -> int:
        """
        从 checkpoint 恢复 page。

        当前逻辑：

            没 checkpoint
                -> page 1

            checkpoint.state["page"]
                -> 从该页继续

        注意：

        这属于 at-least-once 策略。

        同一页可能重新抓一遍，
        但 SeenStore 会负责去重。

        这比错误跳到 page+1
        更安全。
        """

        if checkpoint is None:

            return 1

        state = getattr(
            checkpoint,
            "state",
            None,
        )

        if not isinstance(
            state,
            dict,
        ):

            return 1

        page = state.get(
            "page"
        )

        try:

            page = int(
                page
            )

        except (
            TypeError,
            ValueError,
        ):

            return 1

        if page <= 0:

            return 1

        return page


    def _forum_max_pages(
        self,
        context: CrawlContext | None,
    ) -> int | None:
        """
        获取 forum 最大抓取页数。

        CrawlContext.extra 可以覆盖：

            forum_max_pages

        没指定则使用插件默认值。
        """

        extra = self._context_extra(
            context
        )

        value = extra.get(
            "forum_max_pages"
        )

        if value is None:

            return (
                self.default_forum_max_pages
            )

        if (
            isinstance(
                value,
                str,
            )
            and value.strip().lower()
            in {
                "all",
                "none",
                "unbounded",
            }
        ):

            return None

        try:

            value = int(
                value
            )

        except (
            TypeError,
            ValueError,
        ) as exc:

            raise ValueError(
                "forum_max_pages "
                "must be an integer"
            ) from exc

        if value <= 0:

            raise ValueError(
                "forum_max_pages "
                "must be positive"
            )

        return value

    def _forum_fetch_detail(
        self,
        context: CrawlContext,
    ) -> bool:
        """
        context.metadata 可以覆盖：

            fetch_forum_detail
        """

        metadata = getattr(
            context,
            "metadata",
            None,
        )

        if isinstance(
            metadata,
            dict,
        ):

            value = metadata.get(
                "fetch_forum_detail"
            )

            if value is not None:

                return bool(
                    value
                )

        return (
            self.fetch_forum_detail
        )


    # ========================================================
    # News checkpoint helpers
    # ========================================================

    def _news_start_page(
        self,
        checkpoint: CrawlCheckpoint | None,
    ) -> int:
        """
        Resume news at the last persisted page.

        This deliberately replays the page at-least-once;
        SeenStore handles de-duplication after normalization.
        """

        return self._forum_start_page(
            checkpoint
        )


    def _news_max_pages(
        self,
        context: CrawlContext | None,
    ) -> int | None:

        extra = self._context_extra(
            context
        )

        value = extra.get(
            "news_max_pages"
        )

        if value is None:

            return (
                self.default_news_max_pages
            )

        if (
            isinstance(
                value,
                str,
            )
            and value.strip().lower()
            in {
                "all",
                "none",
                "unbounded",
            }
        ):

            return None

        try:

            value = int(
                value
            )

        except (
            TypeError,
            ValueError,
        ) as exc:

            raise ValueError(
                "news_max_pages "
                "must be an integer"
            ) from exc

        if value <= 0:

            raise ValueError(
                "news_max_pages "
                "must be positive"
            )

        return value


    def _news_mode(
        self,
        context: CrawlContext | None,
    ) -> str:

        extra = self._context_extra(
            context
        )

        value = extra.get(
            "news_mode"
        )

        if value is None:

            return self.news_mode

        value = str(
            value
        ).strip().lower()

        if value not in {
            "full",
            "incremental",
        }:

            raise ValueError(
                "news_mode must be full "
                "or incremental"
            )

        return value


    # ========================================================
    # Research helpers
    # ========================================================

    def _research_start_page(
        self,
        checkpoint: CrawlCheckpoint | None,
    ) -> int:

        return self._forum_start_page(
            checkpoint
        )


    def _research_max_pages(
        self,
        context: CrawlContext | None,
    ) -> int | None:

        extra = self._context_extra(
            context
        )

        value = extra.get(
            "research_max_pages"
        )

        if value is None:

            return (
                self.default_research_max_pages
            )

        if (
            isinstance(
                value,
                str,
            )
            and value.strip().lower()
            in {
                "all",
                "none",
                "unbounded",
            }
        ):

            return None

        try:

            value = int(
                value
            )

        except (
            TypeError,
            ValueError,
        ) as exc:

            raise ValueError(
                "research_max_pages "
                "must be an integer"
            ) from exc

        if value <= 0:

            raise ValueError(
                "research_max_pages "
                "must be positive"
            )

        return value


    def _research_mode(
        self,
        context: CrawlContext | None,
    ) -> str:

        extra = self._context_extra(
            context
        )

        value = extra.get(
            "research_mode"
        )

        if value is None:

            return self.research_mode

        value = str(
            value
        ).strip().lower()

        if value not in {
            "full",
            "incremental",
        }:

            raise ValueError(
                "research_mode must be full "
                "or incremental"
            )

        return value


    def _research_categories(
        self,
        context: CrawlContext | None,
    ):

        extra = self._context_extra(
            context
        )

        return extra.get(
            "research_categories"
        )


    def _download_research_pdf(
        self,
        context: CrawlContext | None,
    ) -> bool:

        extra = self._context_extra(
            context
        )

        value = extra.get(
            "download_research_pdf"
        )

        if value is None:

            return True

        if isinstance(
            value,
            str,
        ):

            return value.strip().lower() not in {
                "0",
                "false",
                "no",
                "off",
            }

        return bool(
            value
        )


    # ========================================================
    # Generic helpers
    # ========================================================

    @staticmethod


    @staticmethod


    @staticmethod


    @staticmethod


    @staticmethod


    @staticmethod
    def _instrument_codes_from_context(
        context: CrawlContext | None,
    ) -> tuple[
        str,
        ...,
    ]:
        """
        从 CrawlContext.extra 获取 instrument codes。

        注意：
        当前真实 CrawlContext 字段是 extra，
        不是 metadata。
        """

        if context is None:

            return ()

        extra = getattr(
            context,
            "extra",
            None,
        )

        if not isinstance(
            extra,
            dict,
        ):

            return ()

        values = extra.get(
            "instrument_codes"
        )

        if values is None:

            return ()

        if isinstance(
            values,
            str,
        ):

            values = (
                values,
            )

        result: list[
            str
        ] = []

        seen: set[
            str
        ] = set()

        for value in values:

            code = str(
                value
            ).strip()

            if not code:

                continue

            if code.upper().startswith(
                "XKRX:"
            ):

                code = code.split(
                    ":",
                    1,
                )[1].strip()

            if code in seen:

                continue

            seen.add(
                code
            )

            result.append(
                code
            )

        return tuple(
            result
        )


    """
    Naver Finance 网站插件。

    当前第一阶段只完成：

        1. SitePlugin 接口适配
        2. dataset 声明
        3. scope / normalize 基础结构
        4. AppFactory 注册能力

    下一阶段逐步迁入真实抓取逻辑：

        forum_post
        news_article
        comment
        instrument

    SitePlugin 只负责：

        discover
        crawl
        normalize
        checkpoint

    不负责：

        SeenStore
        Parquet
        PostgreSQL
        上传
        Recovery
        Cleaner

    这些全部由通用 framework 处理。
    """

    site_id = NAVER_FINANCE_SITE_ID

    country = NAVER_FINANCE_COUNTRY

    timezone = NAVER_FINANCE_TIMEZONE


    # ========================================================
    # Datasets
    # ========================================================

    def datasets(
        self,
    ) -> tuple[
        str,
        ...,
    ]:
        """
        Naver Finance 第一阶段支持的数据集。

        注意：

        这里只声明 framework DATASETS
        中已经注册过的标准 dataset。
        """

        return (
            "forum_post",
            "news_article",
            "news_instrument",
            "research_report",
            "research_instrument",
            "attachment",
        )


    # ========================================================
    # Scope discovery
    # ========================================================

    async def discover(
        self,
        dataset: str,
        context: CrawlContext,
    ) -> AsyncIterator[
        CrawlScope
    ]:
        """
        根据 CrawlContext.extra 中的 instrument_codes
        生成本次抓取 scope。

        例如：

            CrawlContext(
                extra={
                    "instrument_codes": (
                        "005930",
                        "000660",
                    )
                }
            )

        将产生：

            XKRX:005930
            XKRX:000660
        """

        if dataset not in self.datasets():

            raise ValueError(
                f"{self.site_id}: "
                f"unsupported dataset: "
                f"{dataset!r}"
            )

        if dataset in {
            "research_report",
            "research_instrument",
            "attachment",
        }:

            yield CrawlScope(
                scope_type="global",
                source_key="research",
                scope_id=None,
                metadata={
                    "site_id": self.site_id,
                    "dataset": dataset,
                },
            )

            return

        codes = (
            self._instrument_codes_from_context(
                context
            )
        )

        for code in codes:

            instrument_id = (
                self._instrument_id(
                    code
                )
            )

            yield CrawlScope(
                scope_type="instrument",
                source_key=code,
                scope_id=instrument_id,
                metadata={
                    "code": code,
                    "instrument_id": (
                        instrument_id
                    ),
                },
            )

    # ========================================================
    # Crawl
    # ========================================================

    async def crawl(
        self,
        dataset: str,
        scope: CrawlScope,
        checkpoint: CrawlCheckpoint | None,
        context: CrawlContext,
    ) -> AsyncIterator[
        dict[
            str,
            Any,
        ]
    ]:
        """
        Naver Finance 真实抓取入口。

        forum_post:
            使用 NaverForumClient.crawl_pages()
            抓取讨论区列表和帖子详情。

        news_article:
            使用 NaverNewsClient.crawl_pages()
            抓取每只股票的新闻列表与正文。

        forum_post checkpoint:
            如果存在 checkpoint.state["page"]，
            从该页重新开始。

        当前采用 at-least-once 策略：
            同一页允许重复抓取，
            后续由 SeenStore 去重。
        """

        # ====================================================
        # Dataset validation
        # ====================================================

        if dataset not in self.datasets():

            raise ValueError(
                f"{self.site_id}: "
                f"unsupported dataset: "
                f"{dataset!r}"
            )

        # ====================================================
        # Forum posts
        # ====================================================

        if dataset == "forum_post":

            code = self._scope_code(
                scope
            )

            start_page = (
                self._forum_start_page(
                    checkpoint
                )
            )

            max_pages = (
                self._forum_max_pages(
                    context
                )
            )

            fetch_detail = (
                self._forum_fetch_detail(
                    context
                )
            )

            async for raw in (
                self.forum_client
                .crawl_pages(
                    code,
                    start_page=start_page,
                    max_pages=max_pages,
                    fetch_detail=(
                        fetch_detail
                    ),
                )
            ):

                # ---------------------------------------------
                # 防止 client 返回缺少 code。
                # ---------------------------------------------

                if not raw.get(
                    "code"
                ):

                    raw[
                        "code"
                    ] = code

                # ---------------------------------------------
                # 当前 forum client 还没有逐条返回实际 page。
                #
                # 第一版先记录 start_page。
                # 下一步再把 page 从 client 精确传出来。
                # ---------------------------------------------

                if raw.get(
                    "page"
                ) is None:

                    raw[
                        "page"
                    ] = start_page

                yield raw

            return

        # ====================================================
        # News articles
        # ====================================================

        if dataset == "news_article":

            code = self._scope_code(
                scope
            )

            start_page = (
                self._news_start_page(
                    checkpoint
                )
            )

            max_pages = (
                self._news_max_pages(
                    context
                )
            )

            mode = (
                self._news_mode(
                    context
                )
            )

            async for raw in (
                self.news_client
                .crawl_pages(
                    code,
                    start_page=start_page,
                    max_pages=max_pages,
                    mode=mode,
                )
            ):

                if not raw.get(
                    "code"
                ):

                    raw[
                        "code"
                    ] = code

                if raw.get(
                    "page"
                ) is None:

                    raw[
                        "page"
                    ] = start_page

                yield raw

            return

        # ====================================================
        # News instruments
        # ====================================================

        if dataset == "news_instrument":

            code = self._scope_code(
                scope
            )

            start_page = (
                self._news_start_page(
                    checkpoint
                )
            )

            max_pages = (
                self._news_max_pages(
                    context
                )
            )

            async for raw in (
                self.news_client
                .crawl_relation_pages(
                    code,
                    start_page=start_page,
                    max_pages=max_pages,
                )
            ):

                if not raw.get(
                    "code"
                ):

                    raw[
                        "code"
                    ] = code

                if raw.get(
                    "page"
                ) is None:

                    raw[
                        "page"
                    ] = start_page

                yield raw

            return

        # ====================================================
        # Research reports
        # ====================================================

        if dataset == "research_report":

            start_page = (
                self._research_start_page(
                    checkpoint
                )
            )

            max_pages = (
                self._research_max_pages(
                    context
                )
            )

            mode = (
                self._research_mode(
                    context
                )
            )

            categories = (
                self._research_categories(
                    context
                )
            )

            async for raw in (
                self.research_client
                .crawl_pages(
                    categories=categories,
                    start_page=start_page,
                    max_pages=max_pages,
                    mode=mode,
                )
            ):

                if raw.get(
                    "page"
                ) is None:

                    raw[
                        "page"
                    ] = start_page

                yield raw

            return

        # ====================================================
        # Research instruments
        # ====================================================

        if dataset == "research_instrument":

            start_page = (
                self._research_start_page(
                    checkpoint
                )
            )

            max_pages = (
                self._research_max_pages(
                    context
                )
            )

            mode = (
                self._research_mode(
                    context
                )
            )

            categories = (
                self._research_categories(
                    context
                )
            )

            async for raw in (
                self.research_client
                .crawl_relation_pages(
                    categories=categories,
                    start_page=start_page,
                    max_pages=max_pages,
                    mode=mode,
                )
            ):

                if raw.get(
                    "page"
                ) is None:

                    raw[
                        "page"
                    ] = start_page

                yield raw

            return

        # ====================================================
        # Research PDF attachments
        # ====================================================

        if dataset == "attachment":

            if not self._download_research_pdf(
                context
            ):

                return

            if self.attachment_pipeline is None:

                raise RuntimeError(
                    "attachment pipeline is not configured"
                )

            from crawl_framework.core.adapter import (
                AttachmentRequest,
            )

            start_page = (
                self._research_start_page(
                    checkpoint
                )
            )

            max_pages = (
                self._research_max_pages(
                    context
                )
            )

            mode = (
                self._research_mode(
                    context
                )
            )

            categories = (
                self._research_categories(
                    context
                )
            )

            extra = self._context_extra(
                context
            )

            attachment_limit = extra.get(
                "attachment_limit"
            )

            if attachment_limit is not None:

                attachment_limit = int(
                    attachment_limit
                )

                if attachment_limit <= 0:

                    raise ValueError(
                        "attachment_limit must be positive"
                    )

            processed_attachments = 0

            async for raw_report in (
                self.research_client
                .crawl_pages(
                    categories=categories,
                    start_page=start_page,
                    max_pages=max_pages,
                    mode=mode,
                )
            ):

                pdf_url = self._optional_text(
                    raw_report.get(
                        "pdf_url"
                    )
                )

                if not pdf_url:

                    continue

                if (
                    attachment_limit is not None
                    and processed_attachments
                    >= attachment_limit
                ):

                    break

                parent_record = (
                    self._normalize_research_report(
                        raw_report,
                        scope,
                    )
                )

                request = AttachmentRequest(
                    parent_record_uid=(
                        parent_record.record_uid
                    ),
                    source_url=pdf_url,
                    filename=(
                        raw_report.get(
                            "pdf_filename"
                        )
                    ),
                    mime_type="application/pdf",
                    metadata={
                        "site_id": self.site_id,
                        "country": self.country,
                        "dataset": "research_report",
                        "report_id": raw_report.get(
                            "report_id"
                        ),
                        "category": (
                            raw_report.get(
                                "category"
                            )
                            or raw_report.get(
                                "report_type"
                            )
                        ),
                    },
                )

                result = await self.attachment_pipeline.process(
                    request,
                    site_id=self.site_id,
                    country=self.country,
                    dataset="research_report",
                    event_time=parent_record.event_time,
                    require_pdf=True,
                )

                attachment = result.attachment

                yield {
                    "attachment_id": attachment.attachment_id,
                    "parent_record_uid": (
                        attachment.parent_record_uid
                    ),
                    "report_id": raw_report.get(
                        "report_id"
                    ),
                    "category": (
                        raw_report.get(
                            "category"
                        )
                        or raw_report.get(
                            "report_type"
                        )
                    ),
                    "source_url": attachment.source_url,
                    "filename": attachment.filename,
                    "mime_type": attachment.mime_type,
                    "sha256": attachment.sha256,
                    "file_size": attachment.file_size,
                    "local_path": str(
                        attachment.local_path
                    ),
                    "remote_path": (
                        result.upload_result.remote_path
                    ),
                    "upload_status": (
                        result.upload_result.status
                    ),
                    "page": raw_report.get(
                        "page",
                        start_page,
                    ),
                }

                processed_attachments += 1

            return

    # ========================================================
    # Normalize
    # ========================================================

    def normalize(
        self,
        dataset: str,
        raw: dict[
            str,
            Any,
        ],
        scope: CrawlScope,
    ) -> CanonicalRecord | None:
        """
        将 Naver raw 数据转换为 CanonicalRecord。

        当前提供 forum_post / news_article
        的标准字段映射骨架。

        下一阶段真实 crawler 输出 raw dict 后，
        可以直接进入这里。
        """

        if dataset == "forum_post":

            return (
                self._normalize_forum_post(
                    raw,
                    scope,
                )
            )

        if dataset == "news_article":

            return (
                self._normalize_news_article(
                    raw,
                    scope,
                )
            )

        if dataset == "news_instrument":

            return (
                self._normalize_news_instrument(
                    raw,
                    scope,
                )
            )

        if dataset == "research_report":

            return (
                self._normalize_research_report(
                    raw,
                    scope,
                )
            )

        if dataset == "research_instrument":

            return (
                self._normalize_research_instrument(
                    raw,
                    scope,
                )
            )

        if dataset == "attachment":

            return (
                self._normalize_attachment(
                    raw,
                    scope,
                )
            )

        raise ValueError(
            f"{self.site_id}: "
            f"unsupported dataset: "
            f"{dataset!r}"
        )


    # ========================================================
    # forum_post normalize
    # ========================================================

    def _normalize_forum_post(
        self,
        raw: dict[
            str,
            Any,
        ],
        scope: CrawlScope,
    ) -> CanonicalRecord:
        """
        Naver discussion/forum post
        -> CanonicalRecord。

        forum_post 表示“帖子内容实体”。

        设计原则：

        1. nid 决定帖子身份。
        2. title / content / author / written_at
           属于帖子主体。
        3. view_count / recommend / dislike
           属于动态指标，不进入 forum_post payload，
           否则每次浏览量变化都会产生新 version。
        4. detail_url 中的 page 参数只是列表位置，
           不能影响帖子版本。
        5. page 只用于 crawler checkpoint，
           不属于帖子内容。
        """

        # ====================================================
        # Identity
        # ====================================================

        nid = self._required_text(
            raw,
            "nid",
        )

        code = self._scope_code(
            scope
        )

        instrument_id = self._instrument_id(
            code
        )

        # ====================================================
        # Event time
        # ====================================================

        event_time = (
            self._event_time(
                raw.get(
                    "written_at"
                )
                or raw.get(
                    "event_time"
                )
            )
        )

        # ====================================================
        # Stable content fields
        # ====================================================

        title = (
            self._optional_text(
                raw.get(
                    "title"
                )
            )
        )

        content = (
            self._optional_text(
                raw.get(
                    "content"
                )
            )
        )

        author_name = (
            self._optional_text(
                raw.get(
                    "nickname"
                )
            )
        )

        # ====================================================
        # Canonical source URL
        #
        # 不使用 raw["detail_url"]：
        #
        #     ...&page=1
        #     ...&page=2
        #
        # page 会随着帖子在列表中的位置变化，
        # 但帖子身份本身没有变化。
        # ====================================================

        source_url = (
            "https://finance.naver.com/"
            "item/board_read.naver"
            f"?code={code}"
            f"&nid={nid}"
        )

        # ====================================================
        # Stable payload
        #
        # 这里只保留不随抓取时间变化、
        # 且没有在 CanonicalRecord 顶层重复表达的
        # 最小站点身份信息。
        #
        # 以下字段明确不放入 payload：
        #
        #     view_count
        #     recommend
        #     dislike
        #     detail_url
        #     page
        #     content
        #     nickname
        #
        # content / nickname 已进入顶层字段。
        # ====================================================

        payload = {
            "nid": nid,
            "code": code,
        }

        return CanonicalRecord(
            site_id=self.site_id,
            country=self.country,
            dataset="forum_post",
            source_id=nid,
            scope_type=(
                scope.scope_type
            ),
            scope_id=(
                scope.scope_id
                or instrument_id
            ),
            instrument_id=(
                instrument_id
            ),
            event_time=(
                event_time
            ),
            title=(
                title
            ),
            content=(
                content
            ),
            author_name=(
                author_name
            ),
            source_url=(
                source_url
            ),
            payload=(
                payload
            ),
        )

    # ========================================================
    # news normalize
    # ========================================================

    def _normalize_news_article(
        self,
        raw: dict[
            str,
            Any,
        ],
        scope: CrawlScope,
    ) -> CanonicalRecord:
        """
        Naver Finance news article
        -> CanonicalRecord。
        """

        article_id = (
            self._required_text(
                raw,
                "article_id",
            )
        )

        office_id = (
            self._optional_text(
                raw.get(
                    "office_id"
                )
            )
        )

        code = self._scope_code(
            scope
        )

        instrument_id = self._instrument_id(
            code
        )

        source_id = (
            f"{office_id}:{article_id}"
            if office_id
            else article_id
        )

        event_time = (
            self._event_time(
                raw.get(
                    "published_at"
                )
                or raw.get(
                    "event_time"
                )
            )
        )

        return CanonicalRecord(
            site_id=self.site_id,
            country=self.country,
            dataset="news_article",
            source_id=source_id,
            scope_type="global",
            scope_id=None,
            instrument_id=None,
            event_time=event_time,
            title=self._optional_text(
                raw.get(
                    "title"
                )
            ),
            content=self._optional_text(
                raw.get(
                    "content"
                )
                or raw.get(
                    "body"
                )
            ),
            author_name=self._optional_text(
                raw.get(
                    "author"
                )
                or raw.get(
                    "journalist"
                )
            ),
            source_url=self._optional_text(
                raw.get(
                    "canonical_url"
                )
                or raw.get(
                    "canonical"
                )
                or raw.get(
                    "url"
                )
            ),
            payload={
                "article_id": (
                    article_id
                ),
                "office_id": (
                    office_id
                ),
                "code": code,
                "author": (
                    self._optional_text(
                        raw.get(
                            "author"
                        )
                        or raw.get(
                            "journalist"
                        )
                    )
                ),
                "url": (
                    self._optional_text(
                        raw.get(
                            "url"
                        )
                    )
                ),
                "source": (
                    self._optional_text(
                        raw.get(
                            "source"
                        )
                    )
                ),
            },
            relations=[
                RecordRelation(
                    instrument_id=instrument_id,
                    relation_type="primary",
                    confidence=1.0,
                )
            ],
        )


    def _normalize_news_instrument(
        self,
        raw: dict[
            str,
            Any,
        ],
        scope: CrawlScope,
    ) -> CanonicalRecord:

        article_id = self._required_text(
            raw,
            "article_id",
        )

        office_id = self._optional_text(
            raw.get(
                "office_id"
            )
        )

        code = self._scope_code(
            scope
        )

        instrument_id = self._instrument_id(
            code
        )

        article_source_id = (
            f"{office_id}:{article_id}"
            if office_id
            else article_id
        )

        relation_source_id = (
            f"{article_source_id}:"
            f"{instrument_id}"
        )

        return CanonicalRecord(
            site_id=self.site_id,
            country=self.country,
            dataset="news_instrument",
            source_id=relation_source_id,
            scope_type="global",
            scope_id=None,
            instrument_id=instrument_id,
            title=self._optional_text(
                raw.get(
                    "title"
                )
            ),
            source_url=self._optional_text(
                raw.get(
                    "canonical_url"
                )
                or raw.get(
                    "canonical"
                )
                or raw.get(
                    "url"
                )
            ),
            payload={
                "article_source_id": (
                    article_source_id
                ),
                "article_id": (
                    article_id
                ),
                "office_id": (
                    office_id
                ),
                "code": code,
                "instrument_id": (
                    instrument_id
                ),
                "relation_type": "primary",
            },
            relations=[
                RecordRelation(
                    instrument_id=instrument_id,
                    relation_type="primary",
                    confidence=1.0,
                )
            ],
        )


    def _normalize_research_report(
        self,
        raw: dict[
            str,
            Any,
        ],
        scope: CrawlScope,
    ) -> CanonicalRecord:

        report_id = self._required_text(
            raw,
            "report_id",
        )

        raw_instruments = (
            raw.get(
                "instrument_ids"
            )
            or []
        )

        relations = [
            RecordRelation(
                instrument_id=str(
                    instrument_id
                ).strip(),
                relation_type="related",
            )
            for instrument_id in raw_instruments
            if str(
                instrument_id
            ).strip()
        ]

        instrument_id = (
            relations[0].instrument_id
            if len(
                relations
            )
            == 1
            else None
        )

        return CanonicalRecord(
            site_id=self.site_id,
            country=self.country,
            dataset="research_report",
            source_id=report_id,
            scope_type=(
                scope.scope_type
            ),
            scope_id=(
                scope.scope_id
            ),
            instrument_id=instrument_id,
            event_time=self._event_time(
                raw.get(
                    "published_at"
                )
                or raw.get(
                    "event_time"
                )
            ),
            title=self._optional_text(
                raw.get(
                    "title"
                )
            ),
            content=self._optional_text(
                raw.get(
                    "summary"
                )
                or raw.get(
                    "abstract"
                )
                or raw.get(
                    "content"
                )
            ),
            author_name=self._optional_text(
                raw.get(
                    "analyst"
                )
            ),
            source_url=self._optional_text(
                raw.get(
                    "source_url"
                )
                or raw.get(
                    "detail_url"
                )
            ),
            payload={
                "report_id": report_id,
                "category": self._optional_text(
                    raw.get(
                        "category"
                    )
                    or raw.get(
                        "report_type"
                    )
                ),
                "institution": self._optional_text(
                    raw.get(
                        "institution"
                    )
                    or raw.get(
                        "securities_company"
                    )
                ),
                "analyst": self._optional_text(
                    raw.get(
                        "analyst"
                    )
                ),
                "pdf_url": self._optional_text(
                    raw.get(
                        "pdf_url"
                    )
                ),
                "pdf_filename": self._optional_text(
                    raw.get(
                        "pdf_filename"
                    )
                ),
                "summary": self._optional_text(
                    raw.get(
                        "summary"
                    )
                    or raw.get(
                        "content"
                    )
                ),
                "abstract": self._optional_text(
                    raw.get(
                        "abstract"
                    )
                    or raw.get(
                        "content"
                    )
                ),
                "stock_code": self._optional_text(
                    raw.get(
                        "stock_code"
                    )
                ),
                "stock_name": self._optional_text(
                    raw.get(
                        "stock_name"
                    )
                ),
                "classification": self._optional_text(
                    raw.get(
                        "classification"
                    )
                ),
                "views": raw.get(
                    "views"
                ),
                "investment_opinion": self._optional_text(
                    raw.get(
                        "investment_opinion"
                    )
                ),
                "target_price": raw.get(
                    "target_price"
                ),
                "target_price_text": self._optional_text(
                    raw.get(
                        "target_price_text"
                    )
                ),
            },
            relations=relations,
        )


    def _normalize_research_instrument(
        self,
        raw: dict[
            str,
            Any,
        ],
        scope: CrawlScope,
    ) -> CanonicalRecord:

        report_id = self._required_text(
            raw,
            "report_id",
        )

        instrument_id = self._required_text(
            raw,
            "instrument_id",
        )

        relation_source_id = (
            f"{report_id}:"
            f"{instrument_id}"
        )

        return CanonicalRecord(
            site_id=self.site_id,
            country=self.country,
            dataset="research_instrument",
            source_id=relation_source_id,
            scope_type=(
                scope.scope_type
            ),
            scope_id=(
                scope.scope_id
            ),
            instrument_id=instrument_id,
            event_time=self._event_time(
                raw.get(
                    "published_at"
                )
                or raw.get(
                    "event_time"
                )
            ),
            title=self._optional_text(
                raw.get(
                    "title"
                )
            ),
            source_url=self._optional_text(
                raw.get(
                    "source_url"
                )
                or raw.get(
                    "detail_url"
                )
            ),
            payload={
                "report_id": report_id,
                "category": self._optional_text(
                    raw.get(
                        "category"
                    )
                    or raw.get(
                        "report_type"
                    )
                ),
                "instrument_id": instrument_id,
                "stock_code": self._optional_text(
                    raw.get(
                        "stock_code"
                    )
                ),
                "stock_name": self._optional_text(
                    raw.get(
                        "stock_name"
                    )
                ),
                "relation_type": "related",
            },
            relations=[
                RecordRelation(
                    instrument_id=instrument_id,
                    relation_type="related",
                    confidence=1.0,
                )
            ],
        )


    def _normalize_attachment(
        self,
        raw: dict[
            str,
            Any,
        ],
        scope: CrawlScope,
    ) -> CanonicalRecord:

        attachment_id = (
            self._required_text(
                raw,
                "attachment_id",
            )
        )

        return CanonicalRecord(
            site_id=self.site_id,
            country=self.country,
            dataset="attachment",
            source_id=attachment_id,
            scope_type=(
                scope.scope_type
            ),
            scope_id=(
                scope.scope_id
            ),
            title=self._optional_text(
                raw.get(
                    "filename"
                )
            ),
            source_url=self._optional_text(
                raw.get(
                    "source_url"
                )
            ),
            payload={
                "attachment_id": attachment_id,
                "parent_record_uid": (
                    self._required_text(
                        raw,
                        "parent_record_uid",
                    )
                ),
                "report_id": self._optional_text(
                    raw.get(
                        "report_id"
                    )
                ),
                "category": self._optional_text(
                    raw.get(
                        "category"
                    )
                ),
                "filename": self._optional_text(
                    raw.get(
                        "filename"
                    )
                ),
                "mime_type": self._optional_text(
                    raw.get(
                        "mime_type"
                    )
                ),
                "sha256": self._required_text(
                    raw,
                    "sha256",
                ),
                "file_size": raw.get(
                    "file_size"
                ),
                "local_path": self._optional_text(
                    raw.get(
                        "local_path"
                    )
                ),
                "remote_path": self._optional_text(
                    raw.get(
                        "remote_path"
                    )
                ),
                "upload_status": self._optional_text(
                    raw.get(
                        "upload_status"
                    )
                ),
            },
        )


    # ========================================================
    # Helpers
    # ========================================================

    @staticmethod
    def _instrument_id(
        code: str,
    ) -> str:
        """
        韩国股票 canonical instrument id。

        当前统一：

            XKRX:005930
        """

        code = str(
            code
        ).strip()

        if not code:

            raise ValueError(
                "Naver instrument code "
                "cannot be empty"
            )

        if code.upper().startswith(
            "XKRX:"
        ):

            return code

        return (
            f"XKRX:{code}"
        )


    @staticmethod
    def _scope_code(
        scope: CrawlScope,
    ) -> str:

        code = str(
            scope.metadata.get(
                "code"
            )
            or scope.source_key
        ).strip()

        if not code:

            raise ValueError(
                "Naver scope has no "
                "instrument code"
            )

        return code


    @staticmethod
    def _required_text(
        raw: dict[
            str,
            Any,
        ],
        key: str,
    ) -> str:

        value = raw.get(
            key
        )

        if value is None:

            raise ValueError(
                f"missing required "
                f"Naver field: {key}"
            )

        text = str(
            value
        ).strip()

        if not text:

            raise ValueError(
                f"empty required "
                f"Naver field: {key}"
            )

        return text


    @staticmethod
    def _optional_text(
        value: Any,
    ) -> str | None:

        if value is None:

            return None

        text = str(
            value
        ).strip()

        return (
            text
            or None
        )


    @staticmethod
    def _event_time(
        value: Any,
    ) -> datetime:
        """
        第一阶段兼容：

            datetime
            ISO8601 string
            None

        下一阶段接 Naver 韩文时间格式 parser。
        """

        if isinstance(
            value,
            datetime,
        ):

            if (
                value.tzinfo
                is None
            ):

                return value.replace(
                    tzinfo=timezone.utc
                )

            return value

        if isinstance(
            value,
            str,
        ):

            text = value.strip()

            if text:

                try:

                    parsed = (
                        datetime
                        .fromisoformat(
                            text.replace(
                                "Z",
                                "+00:00",
                            )
                        )
                    )

                    if (
                        parsed.tzinfo
                        is None
                    ):

                        parsed = (
                            parsed.replace(
                                tzinfo=(
                                    timezone.utc
                                )
                            )
                        )

                    return parsed

                except ValueError:

                    pass

        # 暂时允许没有时间的数据，
        # 使用当前 UTC。
        #
        # 下一阶段真实 Naver parser
        # 会尽量保证提供 written_at /
        # published_at。
        return datetime.now(
            timezone.utc
        )


        """
        从 CrawlContext 中读取测试/小规模抓取股票代码。

        当前兼容 context.metadata。

        如果当前 CrawlContext 没有 metadata，
        安全返回空 tuple。

        下一阶段会把 instrument discovery
        独立成真正的 Naver scope discovery。
        """

        metadata = getattr(
            context,
            "metadata",
            None,
        )

        if not isinstance(
            metadata,
            dict,
        ):

            return ()

        values = (
            metadata.get(
                "instrument_codes"
            )
        )

        if values is None:

            return ()

        if isinstance(
            values,
            str,
        ):

            values = [
                values
            ]

        result = []

        seen = set()

        for value in values:

            code = str(
                value
            ).strip()

            if not code:

                continue

            if code in seen:

                continue

            seen.add(
                code
            )

            result.append(
                code
            )

        return tuple(
            result
        )
