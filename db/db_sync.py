# This is needed for type hinting with fluent interfaces
from __future__ import annotations

# standard libraries
import logging
from typing import Any, Dict, Sequence

# third-party libraries
from sqlalchemy.orm import sessionmaker

# local libraries
from db.db_creator import DBCreator
import db.schema as SCHEMA


logger = logging.getLogger(__name__)


class DBSync():
    """
    BEWARE: This is untested and not fully implemented code. Don't trust it blindly!

    DBSync instances serves as a way of synchronizing data received from other 
    sources with the database.

    https://docs.python.org/3/tutorial/datastructures.html#sets

    """

    def __init__(
        self,
        records: Sequence[Dict[str, Any]],
        model_name: str,
        filters: Dict[str, Any],
        creator_obj: DBCreator
    ):

        if not hasattr(SCHEMA, model_name):
            logger.error(f'Received an invalid model name: {model_name}')
        
        self.model = getattr(SCHEMA, model_name)
        self.model_pk_str = self.model.__table__.primary_key

        self.new_records: Sequence[Dict[str, Any]] = records
        self.new_pk_values: Sequence[int] = [
            new_record[self.model_pk_str] for new_record in self.new_records
        ]
        logger.info(self.new_pk_values)

        Session: sessionmaker = sessionmaker(bind=creator_obj.engine)
        self.session = Session()

        self.obj_set: Sequence[object] = self.session.query(self.model).all()
        self.old_pk_values = [getattr(obj, self.model_pk_str) for obj in self.obj_set]
        logger.info(self.old_pk_values)

    def update(self) -> DBSync:
        """
        Update records if they are present in both the new and current data.
        This method should be run first to avoid double processing records.
        """
        # Find intersecting records
        old_set = set(self.old_pk_values)
        new_set = set(self.new_pk_values)
        intersecting_pks = list(old_set & new_set)
        logger.debug(intersecting_pks)
        records_to_update = [
            new_record for new_record in self.new_records
            if new_record[self.model_pk_str] in intersecting_pks
        ]

        self.session.bulk_update_mappings(
            self.model,
            records_to_update
        )
        self.session.commit()
        return self

    def create(self) -> DBSync:
        """
        Find records not in self.new_records and create them
        """

        # Find new records
        old_set = set(self.old_pk_values)
        new_set = set(self.new_pk_values)
        nonexistent_pks = list(new_set - old_set)
        logger.debug(nonexistent_pks)
        records_to_create = [
            new_record for new_record in self.new_records
            if new_record[self.model_pk_str] in nonexistent_pks
        ]

        self.session.bulk_insert_mappings(
            self.model,
            records_to_create
        )
        self.session.commit()
        return self

    def delete(self) -> DBSync:
        """
        Find records that aren't in the new records and delete them
        """

        # Find outdated records
        old_set = set(self.old_pk_values)
        new_set = set(self.new_pk_values)
        outdated_pks = tuple(old_set - new_set)
        logger.debug(outdated_pks)

        logger.info(len(self.obj_set))

        self.session.query(self.model)\
            .filter(getattr(self.model, self.model_pk_str))\
            .in_(outdated_pks)\
            .delete()
        self.session.commit()

        logger.info(len(self.session.query(self.model).all()))

        return self
