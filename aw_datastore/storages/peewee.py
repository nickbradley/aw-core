from typing import Optional, List, Dict, Any
from datetime import datetime, timezone
import json
import os
import logging
import iso8601

from peewee import Model, CharField, IntegerField, DecimalField, DateTimeField, ForeignKeyField, PrimaryKeyField
from playhouse.sqlite_ext import SqliteExtDatabase

from aw_core.models import Event
from aw_core.dirs import get_data_dir

from .abstract import AbstractStorage

logger = logging.getLogger(__name__)

# Prevent debug output from propagating
peewee_logger = logging.getLogger("peewee")
peewee_logger.setLevel(logging.INFO)

# Init'd later in the PeeweeStorage constructor.
#   See: http://docs.peewee-orm.com/en/latest/peewee/database.html#run-time-database-configuration
# Another option would be to use peewee's Proxy.
#   See: http://docs.peewee-orm.com/en/latest/peewee/database.html#dynamic-db
_db = SqliteExtDatabase(None)


LATEST_VERSION=2


@_db.func("extract_app")
def extract_app(datastr: str) -> str:
    try:
        data = json.loads(datastr)

        # aw-watcher-window
        if "app" in data:
            return data["app"]
    except:
        return None


@_db.func("extract_path")
def extract_path(datastr: str) -> str:
    try:
        data = json.loads(datastr)

        # aw-watcher-window
        if "app" in data and "title" in data:
            # Extract project from IntelliJ windows
            if data["app"] == "jetbrains-idea":
                path = data["title"].split(" ")[3]
                return path
            else:
                # HACK FOR NOW
                return data["title"]
    except:
        return None


@_db.func("extract_project")
def extract_project(datastr: str) -> str:
    try:
        data = json.loads(datastr)

        # aw-watcher-window
        if "app" in data and "title" in data:

            # Extract project from IntelliJ windows
            if data["app"] == "jetbrains-idea":
                project = data["title"].split(" ")[1]
                if project[0] == "[" and project[-1] == "]":
                    return project[1:-1]
    except:
        return None


# @_db.func("extract_intellij_project_path")
# def extract_intellij_project_path(title: str) -> str:
#     try:
#         project_path = title.split(" ")[1]
#
#         if project_path[0] == "[" and project_path[-1] == "]":
#             return project_path[1:-1]
#     except:
#         return None


def chunks(l, n):
    """Yield successive n-sized chunks from l.
    From: https://stackoverflow.com/a/312464/965332"""
    for i in range(0, len(l), n):
        yield l[i:i + n]


class BaseModel(Model):
    class Meta:
        database = _db


class BucketModel(BaseModel):
    key = IntegerField(primary_key=True)
    id = CharField(unique=True)
    created = DateTimeField(default=datetime.now)
    name = CharField(null=True)
    type = CharField()
    client = CharField()
    hostname = CharField()

    def json(self):
        return {"id": self.id, "created": iso8601.parse_date(self.created).astimezone(timezone.utc).isoformat(),
                "name": self.name, "type": self.type, "client": self.client,
                "hostname": self.hostname}


class EventModel(BaseModel):
    id = PrimaryKeyField()
    bucket = ForeignKeyField(BucketModel, related_name='events', index=True)
    timestamp = DateTimeField(index=True, default=datetime.now)
    duration = DecimalField()
    datastr = CharField()

    @classmethod
    def from_event(cls, bucket_key, event: Event):
        return cls(bucket=bucket_key, id=event.id, timestamp=event.timestamp, duration=event.duration.total_seconds(), datastr=json.dumps(event.data))

    def json(self):
        return {
            "id": self.id,
            "timestamp": self.timestamp,
            "duration": float(self.duration),
            "data": json.loads(self.datastr)
        }


class PeeweeStorage(AbstractStorage):
    sid = "peewee"

    def __init__(self, testing: bool = True, filepath: str = None) -> None:
        data_dir = get_data_dir("aw-server")

        if not filepath:
            filename = 'peewee-sqlite' + ('-testing' if testing else '') + ".v{}".format(LATEST_VERSION) + '.db'
            filepath = os.path.join(data_dir, filename)
        self.db = _db
        self.db.init(filepath)
        logger.info("Using database file: {}".format(filepath))

        self.db.connect()

        self.bucket_keys = {}  # type: Dict[str, int]
        if not BucketModel.table_exists():
            BucketModel.create_table()
        if not EventModel.table_exists():
            EventModel.create_table()
        self.update_bucket_keys()

#         self.db.execute_sql("""
#         create table if not exists resource (
#   id integer PRIMARY KEY AUTOINCREMENT,
#   eventmodel_id integer REFERENCES eventmodel (id),
#   timestamp datetime,
#   duration decimal(10,5),
#   project varchar(255),
#   identifier varchar(255),
#   type varchar(255)
# );
#         """)

        self.db.execute_sql("""
        create table if not exists resource (
          id integer PRIMARY KEY AUTOINCREMENT,
          eventmodel_id integer REFERENCES eventmodel (id),
          
          timestamp datetime,
          duration decimal(10,5),
          tracker_name varchar(255),
          tracker_type varchar(255),
          app_name varchar(255),
          app_title varchar(255),
          reference varchar(255),
          project varchar(255)
        );
        """)


        self.db.execute_sql("""
            create trigger if not exists add_resource
                after insert on eventmodel
                for each row
                    begin
                    insert into resource (eventmodel_id, timestamp, duration, tracker_name, tracker_type, app_name, app_title, reference, project)
                    values (
                        new.id,
                        new.timestamp,
                        new.duration,
                        (
                          select client
                          from bucketmodel
                          where key = new.bucket_id
                        ),  --  as tracker_name
                        (
                            select type
                            from bucketmodel
                            where key = new.bucket_id
                        ), --  as tracker_type
                        (
                            select
                                case type
                                    when 'app.editor.activity' then NULL
                                    when 'web.tab.current' then NULL
                                    when 'currentwindow' then json_extract(new.datastr, '$.app')
                                end
                            from bucketmodel
                            where key = new.bucket_id
                        ),  --  as app_name
                        (
                            select
                                case type
                                    when 'app.editor.activity' then NULL
                                    when 'web.tab.current' then
                                        case bucketmodel.id
                                            when 'aw-watcher-web-firefox' then json_extract(new.datastr, '$.title') || ' - Mozilla Firefox'
                                            when 'aw-watcher-web-chrome' then json_extract(new.datastr, '$.title') || ' - Google Chrome'
                                        end
                                    when 'currentwindow' then json_extract(new.datastr, '$.title')
                                end
                            from bucketmodel
                            where key = new.bucket_id
                        ),  --  as app_title
                        (
                            select
                                case type
                                    when 'app.editor.activity' then json_extract(new.datastr, '$.file')
                                    when 'web.tab.current' then json_extract(new.datastr, '$.url')
                                    when 'currentwindow' then json_extract(new.datastr, '$.title')
                                end
                            from bucketmodel
                            where key = new.bucket_id
                        ),  --  as reference
                        (
                          select json_extract(datastr, '$.projectPath')
                          from eventmodel
                          where json_extract(datastr, '$.project') is not null
                            and timestamp >= datetime('now', '-10 minutes')
                          order by timestamp desc
                          limit 1
                        ) -- as project
                    );
            end;
        """)

        self.db.execute_sql("""
        CREATE TRIGGER if not exists update_duration AFTER UPDATE ON eventmodel
        WHEN OLD.duration != new.duration
        begin
            update resource
            set duration = new.duration
            where eventmodel_id = new.id;
        end;
        """)


        # self.db.execute_sql("""
        # CREATE TABLE IF NOT EXISTS resource (
        #     id            INTEGER PRIMARY KEY AUTOINCREMENT
        #                           NOT NULL,
        #     eventmodel_id INTEGER REFERENCES eventmodel (id)
        #                           NOT NULL,
        #     project       VARCHAR,
        #     app           VARCHAR NOT NULL,
        #     path          VARCHAR NOT NULL
        # );
        # """)
        #
        #
        # self.db.execute_sql("""
        # CREATE TRIGGER IF NOT EXISTS extract_resource
        #          AFTER INSERT
        #             ON eventmodel
        #       FOR EACH ROW
        #       WHEN extract_path(NEW.datastr) IS NOT NULL
        # BEGIN
        #     INSERT INTO resource (
        #          eventmodel_id,
        #          project,
        #          app,
        #          path
        #     )
        #     VALUES (
        #         NEW.id,
        #         (
        #             SELECT extract_project(datastr)
        #             FROM eventmodel
        #             WHERE extract_project(datastr) IS NOT NULL AND
        #                   timestamp >= datetime(NEW.timestamp, '-10 minutes')
        #             ORDER BY timestamp DESC
        #             LIMIT 1
        #         ),
        #         extract_app(NEW.datastr),
        #         extract_path(NEW.datastr)
        #     );
        # END;
        # """)



        # self.db.execute_sql("""
        #     CREATE TABLE IF NOT EXISTS activeproject (
        #         path  VARCHAR (255)           NOT NULL,
        #         start_timestamp DATETIME      NOT NULL
        #                                       DEFAULT (strftime('%Y-%m-%d %H:%M:%f000+00:00', 'now')),
        #         end_timestamp DATETIME        NOT NULL
        #                                       DEFAULT (strftime('%Y-%m-%d %H:%M:%f000+00:00', 'now', '+10 minutes'))
        #     );
        # """)
        #
        # self.db.execute_sql("""
        #     CREATE TRIGGER IF NOT EXISTS active_project
        #     AFTER INSERT
        #         ON eventmodel
        #     FOR EACH ROW
        #     WHEN extract_intellij_project_path(json_extract(NEW.datastr, '$.title') ) IS NOT NULL
        #     BEGIN
        #     INSERT INTO [ActiveProject] (path, start_timestamp, end_timestamp) VALUES (
        #        extract_intellij_project_path(json_extract(NEW.datastr, '$.title')),
        #        NEW.timestamp,
        #        strftime('%Y-%m-%d %H:%M:%f000+00:00', NEW.timestamp, '+10 minutes')
        #     );
        #     END;
        # """)

    def update_bucket_keys(self) -> None:
        buckets = BucketModel.select()
        self.bucket_keys = {bucket.id: bucket.key for bucket in buckets}

    def buckets(self) -> Dict[str, Dict[str, Any]]:
        buckets = {bucket.id: bucket.json() for bucket in BucketModel.select()}
        return buckets

    def create_bucket(self, bucket_id: str, type_id: str, client: str,
                      hostname: str, created: str, name: Optional[str] = None):
        BucketModel.create(id=bucket_id, type=type_id, client=client,
                           hostname=hostname, created=created, name=name)
        self.update_bucket_keys()

    def delete_bucket(self, bucket_id: str):
        EventModel.delete().where(EventModel.bucket == self.bucket_keys[bucket_id]).execute()
        BucketModel.delete().where(BucketModel.key == self.bucket_keys[bucket_id]).execute()
        self.update_bucket_keys()

    def get_metadata(self, bucket_id: str):
        return BucketModel.get(BucketModel.key == self.bucket_keys[bucket_id]).json()

    def insert_one(self, bucket_id: str, event: Event) -> Event:
        e = EventModel.from_event(self.bucket_keys[bucket_id], event)
        e.save()
        event.id = e.id
        return event

    def insert_many(self, bucket_id, events: List[Event], fast=False) -> None:
        events_dictlist = [{"bucket": self.bucket_keys[bucket_id],
                            "timestamp": event.timestamp,
                            "duration": event.duration.total_seconds(),
                            "datastr": json.dumps(event.data)}
                           for event in events]
        # Chunking into lists of length 100 is needed here due to SQLITE_MAX_COMPOUND_SELECT
        # and SQLITE_LIMIT_VARIABLE_NUMBER under Windows.
        # See: https://github.com/coleifer/peewee/issues/948
        for chunk in chunks(events_dictlist, 100):
            EventModel.insert_many(chunk).execute()

    def _get_event(self, bucket_id, event_id) -> EventModel:
        return EventModel.select() \
                         .where(EventModel.id == event_id) \
                         .where(EventModel.bucket == self.bucket_keys[bucket_id]) \
                         .get()

    def _get_last(self, bucket_id) -> EventModel:
        return EventModel.select() \
                         .where(EventModel.bucket == self.bucket_keys[bucket_id]) \
                         .order_by(EventModel.timestamp.desc()) \
                         .get()

    def replace_last(self, bucket_id, event):
        e = self._get_last(bucket_id)
        e.timestamp = event.timestamp
        e.duration = event.duration.total_seconds()
        e.datastr = json.dumps(event.data)
        e.save()
        event.id = e.id
        return event

    def delete(self, bucket_id, event_id):
        return EventModel.delete() \
                         .where(EventModel.id == event_id) \
                         .where(EventModel.bucket == self.bucket_keys[bucket_id]) \
                         .execute()

    def replace(self, bucket_id, event_id, event):
        e = self._get_event(bucket_id, event_id)
        e.timestamp = event.timestamp
        e.duration = event.duration.total_seconds()
        e.datastr = json.dumps(event.data)
        e.save()
        event.id = e.id
        return event

    def get_events(self, bucket_id: str, limit: int,
                   starttime: Optional[datetime] = None, endtime: Optional[datetime] = None):
        if limit == 0:
            return []
        q = EventModel.select() \
                      .where(EventModel.bucket == self.bucket_keys[bucket_id]) \
                      .order_by(EventModel.timestamp.desc()) \
                      .limit(limit)
        if starttime:
            # Important to normalize datetimes to UTC, otherwise any UTC offset will be ignored
            starttime = starttime.astimezone(timezone.utc)
            q = q.where(starttime <= EventModel.timestamp)
        if endtime:
            endtime = endtime.astimezone(timezone.utc)
            q = q.where(EventModel.timestamp <= endtime)
        return [Event(**e) for e in list(map(EventModel.json, q.execute()))]

    def get_eventcount(self, bucket_id: str,
                       starttime: Optional[datetime] = None, endtime: Optional[datetime] = None):
        q = EventModel.select() \
                      .where(EventModel.bucket == self.bucket_keys[bucket_id])
        if starttime:
            # Important to normalize datetimes to UTC, otherwise any UTC offset will be ignored
            starttime = starttime.astimezone(timezone.utc)
            q = q.where(starttime <= EventModel.timestamp)
        if endtime:
            endtime = endtime.astimezone(timezone.utc)
            q = q.where(EventModel.timestamp <= endtime)
        return q.count()
