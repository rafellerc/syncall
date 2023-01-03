from typing import Dict, Optional, Sequence, cast

from bubop import logger
from notional import blocks, types
from notional.session import APIResponseError, Session

from taskwarrior_syncall.notion_todo_db_records import NotionTodoRecord
from taskwarrior_syncall.sync_side import SyncSide
from taskwarrior_syncall.types import NotionID


class NotionDBSide(SyncSide):
    """
    Wrapper class to add/modify/delete rows from todo database to from notion, create new pages, etc.
    """

    _date_keys = "last_modified_date"

    def __init__(self, client: Session, todo_db_id: NotionID, project_db_id: NotionID = None):
        self._client: Session = client
        self._todo_db_id = todo_db_id
        self._project_db_id = project_db_id
        self._all_todo_records: Dict[NotionID, NotionTodoRecord]
        self._is_cached = False

        super().__init__(name="NotionDB", fullname="NotionDB")

    @classmethod
    def id_key(cls) -> str:
        return "id"

    @classmethod
    def summary_key(cls) -> str:
        return "description"

    @classmethod
    def last_modification_key(cls) -> str:
        return "last_modified_date"

    def start(self):
        logger.info(f"Initializing {self.fullname}...")
        # TODO: Check schema
        self._project_short_name_to_page = {
            page.properties["ShortName"].Value:page for page in self._client.databases.query(self._project_db_id).execute()
            if page.properties["ShortName"].Value != ""
            }

    def _get_todo_records(self) -> Dict[NotionID, NotionTodoRecord]:
        query = self._client.databases.query(self._todo_db_id)
        records = [record for record in query.execute()
                   if not record.properties["ExcludeFromTW"].checkbox and
                   record.properties["Status"] not in  ["Discarded", "Done"]
                   ]
        return {cast(NotionID, record.id): NotionTodoRecord.from_record(record)
                for record in records}

    def get_all_items(self, **kargs) -> Sequence[NotionTodoRecord]:
        self._all_todo_records = self._get_todo_records()
        self._is_cached = True

        return tuple(self._all_todo_records.values())

    def get_item(
        self, item_id: NotionID, use_cached: bool = False
    ) -> Optional[NotionTodoRecord]:
        """Return a single todo record"""
        if use_cached:
            return self._all_todo_records.get(item_id)

        # have to fetch and cache it again
        try:
            new_record_block: blocks.Page = self._client.pages.retrieve(item_id)
            new_record = NotionTodoRecord.from_record(new_record_block)
        except APIResponseError:
            raise KeyError

        assert new_record.id is not None
        self._all_todo_records[new_record.id] = new_record

        return new_record

    def delete_single_item(self, item_id: NotionID):
        """Delete a single record."""
        record = self._client.pages.retrieve(item_id)
        self._client.pages.update(record, **{"Status": types.Status.__compose__('Discarded', color='red')})

    def update_item(self, item_id: NotionID, **updated_properties):
        record = self._client.pages.retrieve(item_id)
        self._client.pages.update(record, **NotionTodoRecord.notion_properties_for_updated(updated_properties))

    def add_item(self, new_record: NotionTodoRecord) -> NotionTodoRecord:
        """Add a new record to the database."""
        props = new_record.show_notion_properties(exclude_title=True)
        page = self._client.pages.create(
            parent=types.DatabaseRef(database_id=self._todo_db_id),
            title=new_record.description,
            properties=props
        )
        return NotionTodoRecord.from_record(page)

    @classmethod
    def items_are_identical(
        cls, item1: NotionTodoRecord, item2: NotionTodoRecord, ignore_keys: Sequence[str] = []
    ) -> bool:
        ignore_keys_ = ["last_edited_time"]
        ignore_keys_.extend(ignore_keys)
        return item1.compare(item2, ignore_keys=ignore_keys_)
