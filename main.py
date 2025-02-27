from pathlib import Path
import pandas as pd
import requests
from google.protobuf.json_format import MessageToDict
from google.transit import gtfs_realtime_pb2

URL = "https://bustime.ttc.ca/gtfsrt/vehicles"


def get_realtime_dict() -> dict:
    # requesting TTC data
    res = requests.get(URL)

    # loading protobuf data
    feed = gtfs_realtime_pb2.FeedMessage()
    feed.ParseFromString(res.content)
    # converting to dict on return
    return MessageToDict(feed)


def get_realtime_df(rtdict: dict) -> pd.DataFrame:
    df = pd.json_normalize(rtdict["entity"])
    df = df.drop("id", axis=1)
    df.columns = [c.split(".")[-1] for c in df.columns]
    df.rename(
        columns={
            "occupancyStatus": "occupancy",
            "tripId": "trip_id",
            "scheduleRelationship": "schedule_relationship",
            "routeId": "route_id",
        },
        inplace=True,
    )

    return df


def save(data: pd.DataFrame) -> None:
    path = Path("data.csv")
    header = not path.exists()
    mode = "w" if header else "a"
    path.touch(exist_ok=True)
    data.to_csv(path, mode=mode, header=header, index=False)


def main():
    rt = get_realtime_dict()
    df = get_realtime_df(rt)
    save(df)
    return df


if __name__ == "__main__":
    main()
