namespace Placium.Common;

public interface IConnectionsConfig
{
    string GetConnectionString(string name);
}