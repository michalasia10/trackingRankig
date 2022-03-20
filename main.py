import logging
from functools import wraps
from time import time
from typing import List

from sqlalchemy import func, bindparam
from sqlalchemy.orm import Session

from models import Base, engine, Track, SessionLocal, Sample
from schema import TracksWithCountSchema, TopArtistSchema

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

Base.metadata.create_all(engine)


def timing(function):
    """
    Decorator to determine the duration of the function
    """

    @wraps(function)
    def wrap(*args, **kw):
        start = time()
        result = function(*args, **kw)
        stop = time()
        print("\n", "=" * 60)
        print(f'\n func: {function.__name__} took: {stop - start:0.2f}sec \n')
        print("=" * 60, "\n")
        return result

    return wrap


def open_file(file_path: str):
    """
    Function as generator to yield row from open file
    """
    with open(file_path, encoding='latin-1') as file:
        for line in file:
            yield line.strip().split('<SEP>')


def check_if_table_have_rows(table: Base) -> bool:
    """
    Function to check if any row / entities exist in table
    """
    database: Session
    with SessionLocal.begin() as database:
        return database.query(table).count() > 0


def insert_into_table(table: Base, file_path: str, **kwargs):
    """
    Function to insert values from file to table
    :param table: Table
    :param file_path: file_path as str
    :param kwargs: dict with key as table's column, value as index of split row from file
    """

    binds_param = {key: bindparam(str(key)) for key in kwargs}
    conn = engine.pool._creator()
    cursor = conn.cursor()
    compiled = (table.__table__.insert().values(**binds_param).compile(dialect=engine.dialect))

    if compiled.positional:
        cursor.executemany(
            str(compiled),
            (
                tuple(row[index] for index in kwargs.values()) for row in
                open_file(file_path)
            )
        )

    else:
        cursor.executemany(
            str(compiled),
            (
                {key: row[index] for key, index in kwargs.items()}
                for row in open_file(file_path)
            )
        )

    conn.commit()
    conn.close()
    logger.info("%s were created", table.__tablename__)


@timing
def create_track_table_entities(unique_tracks_path: str):
    """
    Function create new entities in track table.
    """
    if not check_if_table_have_rows(Track):
        logger.info("Track table creation has begun")
        return insert_into_table(Track,
                                 unique_tracks_path,
                                 track_id=0,
                                 work_id=1,
                                 artist_name=2,
                                 track_title=3)

    return logger.warning("Track table have enough entities to query")


@timing
def create_sample_table_entities(sample_path: str):
    """
    Function create new entities in sample table.
    """
    if not check_if_table_have_rows(Sample):
        logger.info("Sample table creation has begun")
        return insert_into_table(Sample,
                                 sample_path,
                                 user_id=0,
                                 track_id=1,
                                 date=2)

    return logger.warning("Sample table have enough entities to query")


def convert_sql_alchemy_object(object_alchemy) -> dict:
    """
    Function to convert sqlalchemy object to dict
    :return: dict
    """
    return {key: value
            for key, value in object_alchemy.__dict__.items()
            if key in ['artist_name', 'track_title']}


@timing
def get_top5_songs() -> List[TracksWithCountSchema]:
    """
    Function to get top 5 songs based on Track, Sample tables
    """
    logger.info("Wait for result from 'get_top_5_songs'")
    database: Session
    with SessionLocal.begin() as database:
        subquery = database.query(Sample,
                                  func.count('*')
                                  .label("sample_count")
                                  ).group_by(Sample.track_id).subquery()

        top5 = database.query(Track,
                              subquery.c.sample_count) \
            .join(subquery) \
            .order_by(subquery.c.sample_count.desc()) \
            .limit(5) \
            .all()

        converted_alchemy_objects = [{"sample_count": track[1],
                                      "track": convert_sql_alchemy_object(track[0])}
                                     for track in top5]

    return [TracksWithCountSchema(**track) for track in converted_alchemy_objects]


@timing
def get_top_artist() -> TopArtistSchema:
    """
    Function to get top artist based on Track, Sample tables
    """
    logger.info("Wait for result from 'get_top_artist'")
    database: Session
    with SessionLocal.begin() as database:
        subquery = database.query(func.count('*').label("sample_count"),
                                  Sample).group_by(Sample.track_id).subquery()

        top_artist = database.query(
            func.sum(subquery.c.sample_count),
            Track.artist_name) \
            .join(Track) \
            .group_by(Track.artist_name) \
            .order_by(func.sum(subquery.c.sample_count).desc()).limit(1).all()

        return TopArtistSchema(artist_name=top_artist[0][1], sample_count=top_artist[0][0])


def pretty_print_top5(tracks: List[TracksWithCountSchema]):
    """
    Function to print result for top5 songs
    """
    print("Top 5 songs".center(50, "="))
    print(f"|Artist Name|{'-' * 5}|Track title|{'-' * 5}|Number of listens|")
    print("=" * 200, "\n")
    for track in tracks:
        unique_track = track.track
        print(f"|{unique_track.artist_name}|"
              f"{'-' * 5}|{unique_track.track_title}|"
              f"{'-' * 5}|{track.sample_count}|")
    print("\n", "=" * 200)


def pretty_print_top_artist(top_artist: TopArtistSchema):
    """
    Function to print result for top artist
    """
    print("=" * 200)
    print("\n", "Top Artist".center(50, "="))
    print(f"|Artist Name|{'-' * 5}|Number of listens|")
    print(f"|{top_artist.artist_name}|{'-' * 5}|{top_artist.sample_count}|")
    print("\n", "=" * 200)


if __name__ == '__main__':
    create_track_table_entities('unique_tracks.txt')
    create_sample_table_entities('triplets_sample_20p.txt')
    top5_songs_list: List[TracksWithCountSchema] = get_top5_songs()
    pretty_print_top5(top5_songs_list)
    top_artist_result: TopArtistSchema = get_top_artist()
    pretty_print_top_artist(top_artist_result)
