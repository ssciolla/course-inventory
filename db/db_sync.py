# This is needed for type hinting with fluent interfaces
from __future__ import annotations

# standard libraries
import copy, logging
from typing import Any, Dict, Sequence

# third-party libraries
import pandas as pd
from sqlalchemy.orm import sessionmaker

# local libraries
from db.db_creator import DBCreator
import db.schema as SCHEMA


logger = logging.getLogger(__name__)


def prepare_data_types_for_orm(record: Dict[str, Any]) -> Dict[str, Any]:
    new_record = copy.deepcopy(record)
    for key in new_record.keys():
        value = new_record[key]
        if isinstance(value, pd.Timestamp):
            new_record[key] = value.to_pydatetime()
        elif pd.isnull(value) and value is not None:
            new_record[key] = None
    return new_record


class DBSync():
    """
    BEWARE: This code is not fully tested. Don't trust it blindly!

    DBSync instances serves as a way of synchronizing data received from other
    sources with the database.

    Right now it only supports entities/tables with application-managed primary keys.

    https://docs.python.org/3/tutorial/datastructures.html#setss
    """

    def __init__(
        self,
        records: Sequence[Dict[str, Any]],
        model_name: str,
        creator_obj: DBCreator
    ):

        if not hasattr(SCHEMA, model_name):
            logger.error(f'Received an invalid model name: {model_name}')

        self.creator: DBCreator = creator_obj
        self.model = getattr(SCHEMA, model_name)

        # TO DO: Check for non-auto-incremented primary key
        self.model_pk_str: str = self.model.__table__.columns.values()[0].name
        self.table_name: str = self.model.__table__.name

        self.new_records: Sequence[Dict[str, Any]] = []
        for record in records:
            self.new_records.append(prepare_data_types_for_orm(record))

        self.new_pk_values: Sequence[int] = [
            new_record[self.model_pk_str] for new_record in self.new_records
        ]
        logger.debug(f'Number of new primary key values: {len(self.new_pk_values)}')

        Session = sessionmaker(bind=creator_obj.engine)
        self.session = Session()

        self.obj_set: Sequence[object] = self.session.query(self.model).all()
        if len(self.obj_set) < 10:
            logger.debug(f'All objects: {self.obj_set}')
        else:
            logger.debug(f'First 10 objects: {self.obj_set[:10]}')

        self.old_pk_values = [getattr(obj, self.model_pk_str) for obj in self.obj_set]

        logger.debug(f'Number of old primary key values: {len(self.old_pk_values)}')

    def update(self) -> DBSync:
        """
        Update records if they are present in both the new and current data.
        This method should be run first to avoid double processing records.
        """

        # Find intersecting records
        old_set = set(self.old_pk_values)
        new_set = set(self.new_pk_values)
        intersecting_pks = list(old_set & new_set)

        records_to_update = [
            new_record for new_record in self.new_records
            if new_record[self.model_pk_str] in intersecting_pks
        ]
        num_records_to_update = len(records_to_update)

        if num_records_to_update == 0:
            logger.debug('There are no records to update; update will be skipped')
            return self

        logger.info(f'Updating {num_records_to_update} {self.table_name} records in DB')
        self.session.bulk_update_mappings(
            self.model,
            records_to_update
        )
        self.session.commit()
        logger.info(f'Updated data in {self.table_name} table in {self.creator.db_name}')
        return self

    def insert(self) -> DBSync:
        """
        Find records not in self.new_records and create them.
        """

        # Find new records
        old_set = set(self.old_pk_values)
        new_set = set(self.new_pk_values)
        nonexistent_pks = list(new_set - old_set)
        records_to_insert = [
            new_record for new_record in self.new_records
            if new_record[self.model_pk_str] in nonexistent_pks
        ]
        num_records_to_insert = len(records_to_insert)

        if num_records_to_insert == 0:
            logger.debug('There are no records to insert; insert will be skipped')
            return self

        logger.info(f'Inserting {num_records_to_insert} {self.table_name} records to DB')
        self.session.bulk_insert_mappings(
            self.model,
            records_to_insert
        )
        self.session.commit()
        logger.info(f'Inserted data to {self.table_name} table in {self.creator.db_name}')
        return self

    def delete(self) -> DBSync:
        """
        Find records that aren't in the new records and delete them.
        """

        # Find outdated records
        old_set = set(self.old_pk_values)
        new_set = set(self.new_pk_values)
        outdated_pks = list(old_set - new_set)

        num_records_to_delete = len(outdated_pks)

        if num_records_to_delete == 0:
            logger.debug('There are no records to delete; delete will be skipped')
            return self

        in_list_filter = getattr(self.model, self.model_pk_str).in_(outdated_pks)

        logger.info(f'Deleting {num_records_to_delete} {self.table_name} records in DB')
        self.session.query(self.model).filter(in_list_filter).delete(synchronize_session=False)

        self.session.commit()
        logger.info(f'Deleted data in {self.table_name} table in {self.creator.db_name}')

        return self

    def sync(self) -> DBSync:
        return self.update().insert().delete()
