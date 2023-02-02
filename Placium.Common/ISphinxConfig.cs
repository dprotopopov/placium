namespace Placium.Common
{
    public interface ISphinxConfig
    {
        string SphinxHttp();
        string GetWordformsPath(string fileName);
    }
}