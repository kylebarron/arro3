import arro3.core
import h3.api.numpy_int as h3
import lonboard
import numpy as np
from arro3.core import Array, Field, fixed_size_list_array, list_array

cell = h3.geo_to_h3(-10, 15, 4)
disk = h3.k_ring(cell, 2)

# Overallocates in case there are any pentagons
coords_per_hexagon = 7
total_coords = len(disk) * coords_per_hexagon
# Allocate coordinate buffer
coords = np.zeros((total_coords, 2), dtype=np.float64)
# Allocate ring offsets. The length should be one greater than the number of rings
ring_offsets = np.zeros(len(disk) + 1, dtype=np.int32)
# In the case of h3 cells, we know that every cell will be a simple polygon with a
# single ring, so we can just use `arange`
geom_offsets = np.arange(len(disk) + 1, dtype=np.int32)

coord_offset = 0
for i, h in enumerate(disk):
    boundary = h3.h3_to_geo_boundary(h, geo_json=True)
    num_coords = len(boundary)
    coords[coord_offset : coord_offset + num_coords, :] = boundary
    coord_offset += num_coords
    ring_offsets[i + 1] = coord_offset

# shrink the coordinates array, in case there are any pentagons
last_coord = ring_offsets[-1]
coords = coords[:last_coord, :]


polygon_array = list_array(geom_offsets, list_array(ring_offsets, coords))
polygon_array_with_geo_meta = polygon_array.cast(
    polygon_array.field.with_metadata({"ARROW:extension:name": "geoarrow.polygon"})
)
lonboard.viz(polygon_array_with_geo_meta)

# Note that this simple implementation probably breaks over the antimeridian.
