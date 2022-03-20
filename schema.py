from pydantic import BaseModel


class TrackSchema(BaseModel):
    artist_name: str
    track_title: str


class TracksWithCountSchema(BaseModel):
    track: TrackSchema
    sample_count: int


class TopArtistSchema(BaseModel):
    artist_name: str
    sample_count: int
