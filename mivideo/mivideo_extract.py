# -*- coding: utf-8 -*-
'''
Module for setting up and running the MiVideo data extract.
'''
import logging
import os
import time
from datetime import datetime
from typing import Dict, Iterable, List, Sequence, Union

import pandas as pd
from KalturaClient import KalturaClient, KalturaConfiguration
from KalturaClient.Plugins.Core import KalturaFilterPager, KalturaMediaEntry, \
    KalturaMediaEntryFilter, KalturaMediaEntryOrderBy, KalturaMediaService, \
    KalturaRequestConfiguration, KalturaSessionService, KalturaSessionType
from KalturaClient.exceptions import KalturaException
from google.cloud import bigquery
from google.oauth2 import service_account
from pandas.io.sql import SQLTable
from sqlalchemy.engine import Connection, ResultProxy
from sqlalchemy.exc import SQLAlchemyError

import mivideo.queries as queries
from db.db_creator import DBCreator
from environ import CONFIG_DIR, CONFIG_PATH, ENV
from vocab import ValidDataSourceName

logger = logging.getLogger(__name__)

SHAPE_ROWS: int = 0  # Index of row count in DataFrame.shape() array


class MiVideoExtract:
    '''
    Initialize the MiVideo data extract process by instantiating the ``MiVideoExtract`` class,
    then invoke its ``run()`` method.

    For example, ``MiVideoExtract().run()``.
    '''

    def __init__(self):
        self.mivideoEnv: Dict = ENV.get('MIVIDEO', {})
        self.__udpInit()
        self.__kalturaInit()
        self.defaultLastTimestamp: str = \
            self.mivideoEnv.get('default_last_timestamp', '2020-03-01 00:00:00+00:00')

        dbParams: Dict = ENV['INVENTORY_DB']
        appendTableNames: List[str] = ENV.get(
            'APPEND_TABLE_NAMES', ['mivideo_media_started_hourly']
        )

        self.appDb: DBCreator = DBCreator(dbParams, appendTableNames)

    def __udpInit(self):
        udpKeyFileName: str = self.mivideoEnv.get('service_account_json_filename')
        if (udpKeyFileName is None):
            errorMessage: str = (f'"MIVIDEO.service_account_json_filename" '
                                 f'was not found in {CONFIG_PATH}')
            logger.error(errorMessage)
            raise ValueError(errorMessage)

        self.udpKeyFilePath: str = os.path.join(CONFIG_DIR, udpKeyFileName)
        logger.debug(f'udpKeyFilePath: "{self.udpKeyFilePath}"')

        self.credentials: service_account.Credentials = (
            service_account.Credentials.from_service_account_file(
                self.udpKeyFilePath,
                scopes=['https://www.googleapis.com/auth/cloud-platform'])
        )

        self.udpDb: bigquery.Client = bigquery.Client(
            credentials=self.credentials,
            project=self.credentials.project_id
        )

        logger.info(f'Connected to BigQuery project: "{self.udpDb.project}"')

    def __kalturaInit(self):
        self.kPartnerId: int = self.mivideoEnv.get('kaltura_partner_id')
        self.kUserId: str = self.mivideoEnv.get('kaltura_user_id')
        self.kUserSecret: str = self.mivideoEnv.get('kaltura_user_secret')
        if (None in (self.kPartnerId, self.kUserId, self.kUserSecret)):
            errorMessage: str = (
                f'"kaltura_partner_id", "kaltura_user_id", and "kaltura_user_secret" '
                f'must be set under the "MIVIDEO" property in {CONFIG_PATH}')
            logger.error(errorMessage)
            raise ValueError(errorMessage)

    def __readTableLastTime(self, tableName: str, tableColumnName: str) -> Union[datetime, None]:
        lastTime: Union[datetime, None]

        try:
            sql: str = f'select max(t.{tableColumnName}) from {tableName} t'
            result: ResultProxy = self.appDb.engine.execute(sql)
            lastTime = result.fetchone()[0]
        except SQLAlchemyError:
            logger.info(f'Error getting max "{tableColumnName}" from "{tableName}"; '
                        'returning None')
            lastTime = None

        return lastTime

    def mediaStartedHourly(self) -> Dict[str, Union[ValidDataSourceName, pd.Timestamp]]:
        """
        Update data from Kaltura Caliper events stored in UDP.

        :return: a dictionary with ValidDataSourceName and last run timestamp
        """

        tableName: str = 'mivideo_media_started_hourly'

        logger.info(f'"{tableName}" - Starting procedure...')

        lastTime: Union[datetime, None] = self.__readTableLastTime(tableName,
                                                                   'event_time_utc_latest')

        if (lastTime):
            logger.info(f'"{tableName}" - Last time found in table: "{lastTime}"')
        else:
            lastTime = datetime.fromisoformat(self.defaultLastTimestamp)
            logger.info(f'"{tableName}" - Last time not found in table; '
                        f'using default time: "{lastTime}"')

        logger.debug(f'"{tableName}" - Running query...')

        dfCourseEvents: pd.DataFrame = self.udpDb.query(
            queries.COURSE_EVENTS, job_config=bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter('startTime', 'DATETIME', lastTime),
                ]
            )
        ).to_dataframe()

        logger.debug(f'"{tableName}" - Completed query.')

        if (not dfCourseEvents.empty):
            logger.info(
                f'"{tableName}" - Number of rows returned: ({dfCourseEvents.shape[SHAPE_ROWS]})')

            logger.debug(f'"{tableName}" - Saving to table...')

            dfCourseEvents.to_sql(tableName, self.appDb.engine, if_exists='append', index=False)

            logger.debug(f'"{tableName}" - Saved.')
        else:
            logger.info(f'"{tableName}" - No rows returned.')

        logger.info(f'"{tableName}" - Procedure complete.')

        logger.info('End of extract')

        return {
            'data_source_name': ValidDataSourceName.UNIZIN_DATA_PLATFORM_EVENTS,
            'data_updated_at': pd.to_datetime(time.time(), unit='s', utc=True)
        }

    @staticmethod
    def __queryRunner(
            pandasTable: SQLTable,
            dbConn: Connection,
            columnNameList: Sequence[str],
            data: Iterable):
        '''
        This handles the fairly rare occurrence of conflicting keys when
        inserting data into a table.
        '''

        tableFullName = pandasTable.name
        if (pandasTable.schema):
            tableFullName = f'{pandasTable.schema}.{tableFullName}'
        columnNames = ', '.join(columnNameList)
        valuePlaceholders = ', '.join(['%s'] * len(columnNameList))

        sql = f'INSERT INTO {tableFullName} ({columnNames}) VALUES ({valuePlaceholders}) ' \
              f'ON DUPLICATE KEY UPDATE {columnNameList[0]}={columnNameList[0]}'  # magic

        dbConn.execute(sql, list(data))

    def mediaCreation(self) -> Dict[str, Union[ValidDataSourceName, pd.Timestamp]]:
        """
        Update data with Kaltura media metadata from Kaltura API.

        :return: a dictionary with ValidDataSourceName and last run timestamp
        """

        KALTURA_MAX_MATCHES_ERROR: str = 'QUERY_EXCEEDED_MAX_MATCHES_ALLOWED'
        procedureName: str = 'mediaCreation'

        kClient: KalturaRequestConfiguration = KalturaClient(KalturaConfiguration())
        kClient.setKs(  # pylint: disable=no-member
            KalturaSessionService(kClient).start(
                self.kUserSecret, self.kUserId, KalturaSessionType.ADMIN, self.kPartnerId))
        kMedia = KalturaMediaService(kClient)

        lastTime: Union[datetime, None] = (
            self.__readTableLastTime('mivideo_media_created', 'created_at'))

        if (lastTime):
            logger.info(f'"{procedureName}" - Last time found in table: "{lastTime}"')
        else:
            lastTime = datetime.fromisoformat(self.defaultLastTimestamp)
            logger.info(f'"{procedureName}" - Last time not found in table; '
                        f'using default time: "{lastTime}"')

        createdAtTimestamp: float = lastTime.timestamp()

        kFilter = KalturaMediaEntryFilter()
        kFilter.createdAtGreaterThanOrEqual = createdAtTimestamp
        kFilter.categoriesFullNameIn = 'Canvas_UMich'
        kFilter.orderBy = KalturaMediaEntryOrderBy.CREATED_AT_ASC

        kPager = KalturaFilterPager()
        kPager.pageSize = 500  # 500 is maximum
        kPager.pageIndex = 1

        results: Sequence[KalturaMediaEntry] = None
        lastCreatedAtTimestamp: Union[float, int] = createdAtTimestamp
        lastId: Union[str, None] = None
        numberResults: int = 0
        queryPageNumber: int = kPager.pageIndex  # for logging purposes
        totalNumberResults: int = numberResults  # for logging purposes

        while True:
            try:
                results = kMedia.list(kFilter, kPager).objects
            except KalturaException as kException:
                if (KALTURA_MAX_MATCHES_ERROR in kException.args):
                    # set new filter timestamp, reset pager to page 1, then continue
                    kFilter.createdAtGreaterThanOrEqual = lastCreatedAtTimestamp
                    logger.debug(f'new filter timestamp: ({kFilter.createdAtGreaterThanOrEqual})')

                    # to avoid dupes, also filter out the last ID returned by previous query
                    # because Kaltura compares createdAt greater than *or equal* to timestamp
                    kFilter.idNotIn = lastId
                    kPager.pageIndex = 1
                    continue

                logger.debug(f'Other Kaltura API error: "{kException}"')
                break

            numberResults = len(results)
            logger.debug(f'Query page ({queryPageNumber}); number of results: ({numberResults})')

            if (numberResults > 0):
                resultDictionaries: Sequence[Dict] = tuple(r.__dict__ for r in results)

                creationData: pd.DataFrame = self.__makeCreationData(resultDictionaries)

                creationData.to_sql(
                    'mivideo_media_created', self.appDb.engine, if_exists='append', index=False)

                courseData: pd.DataFrame = self.__makeCourseData(resultDictionaries)

                courseData.to_sql('mivideo_media_courses', self.appDb.engine, if_exists='append',
                                  index=False, method=self.__queryRunner)

                lastCreatedAtTimestamp = results[-1].createdAt
                lastId = results[-1].id
                totalNumberResults += numberResults

            if (numberResults < kPager.pageSize):
                break

            kPager.pageIndex += 1
            queryPageNumber += 1

        logger.info(f'Total number of results: ({totalNumberResults})')

        return {
            'data_source_name': ValidDataSourceName.KALTURA_API,
            'data_updated_at': pd.to_datetime(time.time(), unit='s', utc=True)
        }

    @staticmethod
    def __makeCourseData(resultDictionaries) -> pd.DataFrame:
        courseData: pd.DataFrame = pd.DataFrame.from_records(
            resultDictionaries, columns=('id', 'categories',)
        ).rename(columns={'id': 'media_id', 'categories': 'course_id', })

        courseData = courseData.assign(
            course_id=courseData['course_id'].str.split(',')).explode('course_id')

        courseData['in_context'] = [
            c.endswith('>InContext') for c in courseData['course_id']]

        courseData['course_id'] = courseData['course_id'].str \
            .replace(r'^.*>([0-9]+).*$', lambda m: m.groups()[0], regex=True)

        # find and drop non-decimal course IDs
        # (e.g., like category "Canvas_UMich>site>channels>Shared Repository")
        badCourseIds = \
            courseData[courseData['course_id'].str.contains('[^0-9]', regex=True)]
        if (badCourseIds.any):
            logger.debug(f'Non-numeric course IDs to be removed:\n{badCourseIds}')

        courseData = courseData[courseData['course_id'].str.isdecimal()].drop_duplicates()

        return courseData

    @staticmethod
    def __makeCreationData(resultDictionaries) -> pd.DataFrame:
        creationData: pd.DataFrame = pd.DataFrame.from_records(
            resultDictionaries, columns=('id', 'createdAt', 'name', 'duration',)
        ).rename(columns={'createdAt': 'created_at'})

        creationData['created_at'] = pd.to_datetime(creationData['created_at'], unit='s')

        return creationData

    def run(self) -> Sequence[Dict[str, Union[ValidDataSourceName, pd.Timestamp]]]:
        '''
        The main controller that runs each method required to update the data.

        :return: List of dictionaries (keys 'data_source_name' and 'data_updated_at')
        '''
        return [
            # self.mediaStartedHourly(),
            self.mediaCreation(),
        ]


def main() -> Sequence[Dict[str, Union[ValidDataSourceName, pd.Timestamp]]]:
    '''
    This method is invoked when its module is executed as a standalone program.

    :return: Sequence of result dictionaries, with ValidDataSourceName and last run timestamp
    '''
    return MiVideoExtract().run()


if '__main__' == __name__:
    main()
