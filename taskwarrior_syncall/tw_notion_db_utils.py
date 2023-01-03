"""Notion-related utils."""
import datetime
from typing import Dict

from bubop import format_datetime_tz, parse_datetime

from taskwarrior_syncall.notion_todo_db_records import NotionTodoRecord
from taskwarrior_syncall.types import TwItem, NotionID


NOTION_STATUSES_TO_TW = {
    'Discarded': "deleted",
    'Freshly added': "pending",
    'Blocked': "pending",
    'Started': "pending",
    'Waiting': "pending",
    'Allocated for Today': "pending",
    'In Progress': "pending",
    'Done': 'completed'
}

TW_STATUSES_TO_NOTION = {
    "deleted": 'Discarded',
    "pending": 'Freshly added',
    'completed': 'Done'
}

def convert_custom_tw_to_notion_db(tw_item: TwItem, project_id_to_short_name: Dict[NotionID, str]) -> NotionTodoRecord:
    modified = tw_item["modified"]
    if isinstance(modified, datetime.datetime):
        dt = modified
    else:
        dt = parse_datetime(modified)

    short_name_to_project_id = {
        shortn: proid for proid, shortn in project_id_to_short_name.items()
        if shortn != ""
    }
    import pudb; pudb.set_trace()
    project_name = tw_item.get("project", None)
    project_name = project_name if project_name != "" else None

    project_id = short_name_to_project_id.get(project_name, None)

    return NotionTodoRecord(
        last_modified_date=dt,
        description=tw_item["description"],
        project_id=project_id,
        # tags=tw_item.get("tags", []),
        status=TW_STATUSES_TO_NOTION[tw_item.get("status")],
        estimated_time=tw_item.get("oestimate", None)
    )


def convert_notion_db_to_custom_tw(todo_record: NotionTodoRecord, project_id_to_short_name: Dict[NotionID, str]) -> TwItem:
    # TODO: Implement pending check
    return {
        "status": NOTION_STATUSES_TO_TW[todo_record.status],
        "description": todo_record.description,
        # "tags": todo_record.tags,
        "oestimate": todo_record.estimated_time,
        "modified": format_datetime_tz(todo_record.last_modified_date),
        "notiontaskurl": todo_record.url,
        "project": project_id_to_short_name.get(todo_record.project_id, ""),
        "sync": "notion"
    }