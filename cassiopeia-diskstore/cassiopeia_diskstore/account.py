from typing import Type, TypeVar, MutableMapping, Any, Iterable

from datapipelines import DataSource, DataSink, PipelineContext, Query, NotFoundError, \
    validate_query

from cassiopeia.data import Platform, Region
from cassiopeia.dto.account import AccountDto
from cassiopeia.datastores.uniquekeys import convert_to_continent
from .common import SimpleKVDiskService

T = TypeVar("T")


class AccountDiskService(SimpleKVDiskService):
    @DataSource.dispatch
    def get(self, type: Type[T], query: MutableMapping[str, Any],
            context: PipelineContext = None) -> T:
        pass

    @DataSource.dispatch
    def get_many(self, type: Type[T], query: MutableMapping[str, Any],
                 context: PipelineContext = None) -> Iterable[T]:
        pass

    @DataSink.dispatch
    def put(self, type: Type[T], item: T, context: PipelineContext = None) -> None:
        pass

    @DataSink.dispatch
    def put_many(self, type: Type[T], items: Iterable[T], context: PipelineContext = None) -> None:
        pass

    ############
    # Account #
    ############

    _validate_get_account_query = (
        Query.has("puuid")
        .as_(str)
        .or_("gameName")
        .as_(str)
        .and_("tagLine")
        .as_(str)
        .also.has("region")
        .as_(Region)
    )

    @get.register(AccountDto)
    @validate_query(_validate_get_account_query, convert_to_continent)
    def get_account(self, query: MutableMapping[str, Any],
                    context: PipelineContext = None) -> AccountDto:
        continent_str = query["continent"].value

        # Need to hash the name and tag because they can have invalid characters.
        game_name = query.get("name", "").replace(" ", "").lower()
        game_name = str(game_name.encode("utf-8"))
        tag_line = query.get("name", "").replace(" ", "").lower()
        tag_line = str(tag_line.encode("utf-8"))

        for key in self._store:
            if key.startswith("AccountDto."):
                _, continent, puuid, gameName, tagLine = key.split(".")
                if continent == continent_str and any([
                    str(query.get("puuid", None)).startswith(puuid),
                    gameName == game_name,
                    tagLine == tag_line,
                ]):
                    dto = AccountDto(self._get(key))

                    dto_name = dto["gameName"].replace(" ", "").lower()
                    dto_name = str(dto_name.encode("utf-8"))
                    dto_tag = dto["tagLine"].replace(" ", "").lower()
                    dto_tag = str(dto_tag.encode("utf-8"))

                    if any([
                        dto["puuid"] == str(query.get("puuid", None)),
                        dto_name == game_name,
                        dto_tag == tag_line,
                    ]):
                        return dto
        else:
            raise NotFoundError

    @put.register(AccountDto)
    def put_account(self, item: AccountDto, context: PipelineContext = None) -> None:
        continent = Region(item["region"]).continent.value

        name = item["gameName"].replace(" ", "").lower()
        name = name.encode("utf-8")
        tag = item["tagLine"].replace(" ", "").lower()
        tag = tag.encode("utf-8")

        key = "{clsname}.{continent}.{puuid}.{gameName}.{tagLine}".format(
            clsname=AccountDto.__name__,
            continent=continent,
            puuid=item["puuid"][:8],
            gameName=name,
            tagLine=tag
        )

        self._put(key, item)
