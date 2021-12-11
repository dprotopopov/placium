dotnet placium.dll --service=FileOsmUploadService --action=install --file="D:\Projects\placium\upload\Moscow.osm.pbf" --OsmConnection="Host=localhost;Port=5432;Database=Osm;Username=postgres;Password=password;Command Timeout=0"

dotnet placium.dll --service=DatabasePlacexUpdateService --action=update --full=yes --OsmConnection="Host=localhost;Port=5432;Database=Osm;Username=postgres;Password=password;Command Timeout=0"

dotnet placium.dll --service=DatabaseAddrxUpdateService --action=update --full=yes --OsmConnection="Host=localhost;Port=5432;Database=Osm;Username=postgres;Password=password;Command Timeout=0"

dotnet placium.dll --service=SphinxAddrxUpdateService --action=update --full=yes --OsmConnection="Host=localhost;Port=5432;Database=Osm;Username=postgres;Password=password;Command Timeout=0" --SphinxConnection="Host=127.0.0.1;Port=9306;Command Timeout=0"
