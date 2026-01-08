import osmium
from shapely.geometry import Point, Polygon
from shapely.prepared import prep

# Approximate bounding boxes for Senegal and Gambia
# Senegal: roughly between -17.5 to -11.3 longitude, 12.3 to 16.7 latitude
# Gambia: roughly between -16.8 to -13.8 longitude, 13.1 to 13.8 latitude

def create_country_polygons():
    # Senegal bounding box (approximate)
    senegal_coords = [
        (-17.5, 12.3), (-11.3, 12.3), (-11.3, 16.7), (-17.5, 16.7), (-17.5, 12.3)
    ]
    
    # Gambia bounding box (approximate) 
    gambia_coords = [
        (-16.8, 13.1), (-13.8, 13.1), (-13.8, 13.8), (-16.8, 13.8), (-16.8, 13.1)
    ]
    
    senegal_poly = prep(Polygon(senegal_coords))
    gambia_poly = prep(Polygon(gambia_coords))
    
    return senegal_poly, gambia_poly

senegal_poly, gambia_poly = create_country_polygons()

class CountrySplitter(osmium.SimpleHandler):
    def __init__(self):
        super().__init__()
        self.sn_writer = osmium.SimpleWriter("senegal.osm.pbf")
        self.gm_writer = osmium.SimpleWriter("gambia.osm.pbf")
        self.processed_nodes = 0
        self.processed_ways = 0

    def node(self, n):
        if not n.location.valid():
            return

        point = Point(n.location.lon, n.location.lat)
        self.processed_nodes += 1

        if self.processed_nodes % 10000 == 0:
            print(f"Processed {self.processed_nodes:,} nodes...")

        if senegal_poly.contains(point):
            self.sn_writer.add_node(n)
        elif gambia_poly.contains(point):
            self.gm_writer.add_node(n)

    def way(self, w):
        if not w.nodes:
            return
            
        self.processed_ways += 1
        if self.processed_ways % 1000 == 0:
            print(f"Processed {self.processed_ways:,} ways...")
            
        # Check first node to determine which country
        try:
            first_node = w.nodes[0]
            if hasattr(first_node, 'location') and first_node.location.valid():
                point = Point(first_node.location.lon, first_node.location.lat)
                
                if senegal_poly.contains(point):
                    self.sn_writer.add_way(w)
                elif gambia_poly.contains(point):
                    self.gm_writer.add_way(w)
        except:
            pass  # Skip ways with invalid locations

    def relation(self, r):
        # Add relations to both files for now
        self.sn_writer.add_relation(r)
        self.gm_writer.add_relation(r)

    def close(self):
        self.sn_writer.close()
        self.gm_writer.close()

if __name__ == "__main__":
    print("Starting PBF file splitting...")
    print("Using approximate bounding boxes for Senegal and Gambia")
    
    handler = CountrySplitter()
    handler.apply_file("senegal-and-gambia.osm.pbf", locations=True)
    handler.close()
    
    print(f"\nSplitting complete!")
    print(f"Processed {handler.processed_nodes:,} nodes and {handler.processed_ways:,} ways")
    print("Created: senegal.osm.pbf and gambia.osm.pbf")