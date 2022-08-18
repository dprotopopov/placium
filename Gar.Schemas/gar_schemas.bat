for /f %%f in ('dir /b .\gar_schemas\*.xsd') do "C:\Program Files (x86)\Microsoft SDKs\Windows\v10.0A\bin\NETFX 4.8 Tools\xsd.exe" /classes .\gar_schemas\%%f /namespace:%%f
