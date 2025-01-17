import datetime
from dataclasses import dataclass
from typing import Any, Mapping, Optional, Sequence, Union

from bubop import is_same_datetime, logger
from notional import types, blocks

from taskwarrior_syncall.types import (
    NotionID,
)


# NOTE: Keep this dictinary up to date
NOTION_STATUSES = {
    'Discarded': 'red',
    'Freshly added': 'default',
    'Blocked': 'orange',
    'Started': 'default',
    'Waiting': 'yellow',
    'Allocated for Today': 'purple',
    'In Progress': 'blue',
    'Done': 'green'
}


def get_content_from_notion_block(block):
    if isinstance(block, types.MultiSelect):
        return [select.name for select in block.multi_select]
    elif isinstance(block, types.RichText):
        return block.Value
    elif isinstance(block, types.Title):
        return block.Value
    elif isinstance(block, types.Status):
        return block.Value
    elif isinstance(block, types.Date):
        if type(block.Start) == datetime.date:
            start = datetime.datetime.combine(block.Start,
                                              datetime.datetime.min.time(),
                                              tzinfo=datetime.timezone(datetime.timedelta(hours=-3)))
            return start
        return block.Start
    else:
        raise Exception("Type: {} not supported".format(type(block)))


def get_property_from_content(content, notion_type):
    if notion_type == types.Title:
        assert isinstance(content, str)
        return types.Title.__compose__(content)
    elif notion_type == types.RichText:
        # TODO: Support for RichTextObject?
        content = content if content is not None else ""
        assert isinstance(content, str)
        return types.RichText.__compose__(content)
    elif notion_type == types.MultiSelect:
        assert isinstance(content, list)
        return types.MultiSelect.__compose__(content)
    elif notion_type == types.Status:
        return types.Status.__compose__(content, color=NOTION_STATUSES[content])
    elif notion_type == types.Relation:
        content = content if content is not None else []
        return types.Relation.__compose__(content)
    elif notion_type == types.Date:
        assert isinstance(content, datetime.datetime) or content is None
        content = content.astimezone() if content else None
        return types.Date.__compose__(content)
    else:
        raise Exception(f"Type: {notion_type} not supported") 


def valid_tw_duration(durat: Union[str, None]) -> bool:
    if durat is None:
        return True
    elif "P" not in durat or "T" not in durat:
        return False
    else:
        return True

@dataclass
class NotionTodoRecord(Mapping):
    last_modified_date: datetime.datetime
    description: str
    project_id: Optional[NotionID] = None
    # tags: Optional[Sequence[str]] = field(default_factory=list)
    estimated_time: Optional[str] = None
    status: Optional[str] = None
    due_date: Optional[datetime.datetime] = None
    url: Optional[str] = None
    id: Optional[NotionID] = None
    
    _key_names = {
        "last_modified_date",
        "id",
        "description",
        "project_id",
        # "tags",
        "due_date",
        "status",
        "url",
        "estimated_time",

    }

    _date_key_names = {"last_modified_date", "due_date"}

    _list_key_names = {
        # "tags"
        }

    _notion_property_mapping = dict(
        description = ("Description", types.Title),
        project_id = ("Project", types.Relation),
        # tags = ("Tags", types.MultiSelect),
        estimated_time = ("EstimatedTime", types.RichText),
        status = ("Status", types.Status),
        due_date = ("DueDate", types.Date)

    )

    def compare(self, other: "NotionTodoRecord", ignore_keys: Sequence[str] = []) -> bool:
        """Compare two items, return True if they are considered equal."""
        for key in self._key_names:
            if key in ignore_keys:
                continue
            elif key in self._date_key_names:
                if bool(self[key]) !=  bool(other[key]):
                    return False
                elif (self[key] is None) and (other[key] is None):
                    continue
                elif not is_same_datetime(
                    self[key], other[key], tol=datetime.timedelta(minutes=10)
                ):
                    logger.opt(lazy=True).trace(
                        f"\n\nItems differ\n\nItem1\n\n{self}\n\nItem2\n\n{other}\n\nKey"
                        f" [{key}] is different - [{repr(self[key])}] | [{repr(other[key])}]"
                    )
                    return False
            elif key in self._list_key_names:
                return set(self[key]) == set(other[key])
            else:
                if self[key] != other[key]:
                    logger.opt(lazy=True).trace(f"Items differ [{key}]\n\n{self}\n\n{other}")
                    return False

        return True

    def __getitem__(self, key) -> Any:
        return getattr(self, key)

    def __iter__(self):
        for k in self._key_names:
            yield k

    def __len__(self):
        return len(self._key_names)


    @classmethod
    def from_record(cls, record: blocks.Page):
        project_ids = [pro.id for pro in record.properties["Project"].relation]
        project_id = project_ids[0] if len(project_ids) > 0 else None
        description = get_content_from_notion_block(record.properties["Description"])
        oestimate = get_content_from_notion_block(record.properties["EstimatedTime"])
        # Clean oestimate
        if oestimate == "":
            oestimate = None
        if not valid_tw_duration(oestimate):
            raise Exception(f"Invalid oestimate: {oestimate} | in id <{record.id}> description <{description}>")
        return cls(
            last_modified_date = record.last_edited_time,
            description = description,
            project_id = project_id,
            # tags = get_content_from_notion_block(record.properties["Tags"]),
            estimated_time = oestimate,
            id = record.id,
            url = record.url,
            status = get_content_from_notion_block(record.properties["Status"]),
            due_date = get_content_from_notion_block(record.properties["DueDate"]),
        )

    @classmethod
    def notion_properties_for_updated(cls, updated_properties):
        prop_dict = {}
        updated_properties.pop("url", None)
        for name, content in updated_properties.items():
            if name in cls._notion_property_mapping:
                prop_name, prop_type = cls._notion_property_mapping[name]
                prop_dict[prop_name] = get_property_from_content(content, prop_type)
        return prop_dict

    def show_notion_properties(self, exclude_title=False):
        prop_dict = {}
        for name, prop in self._notion_property_mapping.items():
            prop_name, prop_type = prop
            if exclude_title and prop_type == types.Title:
                pass
            else:
                prop_dict[prop_name] = get_property_from_content(self.__getattribute__(name), prop_type)
        return prop_dict

