using System.Threading.Tasks;
using Microsoft.AspNetCore.SignalR;

namespace Placium.Common
{
    public interface IProgressHubClient
    {
        Task Progress(float progress, string id, string session);
        Task Init(string id, string session);
        Task Complete(string session);
        Task Error(string message, string session);
    }

    public class ProgressHub : Hub<IProgressHubClient>
    {
    }
}