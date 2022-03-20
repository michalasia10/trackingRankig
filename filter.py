from collections import Counter
from dataclasses import dataclass
from functools import wraps
from pathlib import Path
from time import time
from typing import Union, List
from uuid import UUID

import pandas as pd


@dataclass
class TrackExis:
    id_user: UUID
    id_track: UUID
    data: int


def timing(f):
    @wraps(f)
    def wrap(*args, **kw):
        ts = time()
        result = f(*args, **kw)
        te = time()
        print("="*60)
        print(f'\n func: {f.__name__} took: {te - ts:0.2f}sec \n')
        return result

    return wrap


def create_object_list(file: Union[str, Path], object_class) -> List[TrackExis]:
    with open(file, encoding='latin-1') as f:
        lines: list = [line.strip().split('<SEP>') for line in f]
        return list(map(lambda x: object_class(*x), lines))


def song_counter(songs) -> Union[dict, Counter]:
    return Counter(track.id_track for track in songs)


@timing
def top_n_songs_for_artis(unique_tracks: Union[str, Path], listenings: Union[str, Path], track_class,
                          n: int) -> (pd.DataFrame,pd.DataFrame):
    track_list = create_object_list(listenings, track_class)
    songs = song_counter(track_list)
    df = pd.read_csv(unique_tracks, encoding='latin-1', sep='<SEP>',
                     names=['id_execution', 'id_track', 'artist_name', 'track_name'])
    df['number_of_listens'] = df.id_track.map(songs)
    return df.sort_values(by=['number_of_listens'], ascending=False).head(n), df


@timing
def top_artist_in_frame(df: pd.DataFrame) -> pd.DataFrame:
    df: pd.DataFrame = df.groupby('artist_name')['number_of_listens'].sum()
    return df.sort_values(ascending=False).head(1)


def printer(df: pd.DataFrame):
    print(df.to_string())


if __name__ == '__main__':
    top_5_songs,df = top_n_songs_for_artis('unique_tracks.txt', 'triplets_sample_20p.txt', TrackExis, 5)
    printer(top_5_songs)
    top_artist = top_artist_in_frame(df)
    printer(top_artist)
