#!/usr/bin/env python3

from fastapi import FastAPI

app = FastAPI()

@app.get("/physical-route/")
def physical_route(src_latitude: float, src_longitude: float,
                   dst_latitude: float, dst_longitude: float) -> list[tuple[float, float]]:
    """Get the physical route in (lat, lon) format from src to dst, including both ends.

    Returns:
        A list of (lat, lon) tuples, one for each hop along the route. This should include both ends.
    """
    # TODO: get the real physical routes from database.
    return [
        (src_latitude, src_longitude),
        (0, 0),
        (dst_latitude, dst_longitude),
    ]

def run():
    import uvicorn
    uvicorn.run(app, port=8082)

if __name__ == "__main__":
    run()
